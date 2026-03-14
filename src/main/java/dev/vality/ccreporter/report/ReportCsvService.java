package dev.vality.ccreporter.report;

import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.PaymentsSearchFilter;
import dev.vality.ccreporter.WithdrawalsQuery;
import dev.vality.ccreporter.WithdrawalsSearchFilter;
import dev.vality.ccreporter.config.ReportTransactionConfig.ReportCsvReadOnlyTxTemplate;
import dev.vality.ccreporter.model.ClaimedReportJob;
import dev.vality.ccreporter.model.GeneratedCsvReport;
import dev.vality.ccreporter.serde.json.ReportQueryJsonSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@RequiredArgsConstructor
public class ReportCsvService {

    private static final DateTimeFormatter CSV_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter CSV_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int CSV_QUERY_FETCH_SIZE = 1_000;

    private static final List<String> PAYMENT_COLUMNS = List.of(
            "created_date",
            "created_time",
            "finalized_date",
            "finalized_time",
            "invoice_id",
            "payment_id",
            "status",
            "amount",
            "currency",
            "trx_id",
            "provider_id",
            "terminal_id",
            "shop_id",
            "exchange_rate_internal",
            "provider_amount",
            "provider_currency",
            "original_amount",
            "original_currency",
            "converted_amount"
    );

    private static final List<String> WITHDRAWAL_COLUMNS = List.of(
            "created_date",
            "created_time",
            "finalized_date",
            "finalized_time",
            "withdrawal_id",
            "status",
            "amount",
            "currency",
            "trx_id",
            "provider_id",
            "terminal_id",
            "wallet_id",
            "exchange_rate_internal",
            "provider_amount",
            "provider_currency",
            "original_amount",
            "original_currency",
            "converted_amount"
    );

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ReportQueryJsonSerializer reportQueryJsonSerializer;
    private final ReportCsvReadOnlyTxTemplate readOnlyRepeatableReadTx;

    public GeneratedCsvReport generate(ClaimedReportJob claimedReportJob) {
        return Objects.requireNonNull(readOnlyRepeatableReadTx.execute(status -> {
            var snapshotFixedAt = currentSnapshot();
            var reportQuery = reportQueryJsonSerializer.deserialize(claimedReportJob.queryJson());
            var zoneId = ZoneId.of(claimedReportJob.timezone());
            var fileName = claimedReportJob.reportType().name() + "-report-" + claimedReportJob.id() + ".csv";
            var stagedFile = createTempFile(claimedReportJob.id());
            try {
                var md5 = createDigest("MD5");
                var sha256 = createDigest("SHA-256");
                var rowsCount = 0L;
                try (
                        var fileOutputStream = Files.newOutputStream(stagedFile);
                        var bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                        var md5OutputStream = new DigestOutputStream(bufferedOutputStream, md5);
                        var sha256OutputStream = new DigestOutputStream(md5OutputStream, sha256);
                        var writer = new BufferedWriter(
                                new OutputStreamWriter(sha256OutputStream, StandardCharsets.UTF_8)
                        )
                ) {
                    if (reportQuery.isSetPayments()) {
                        rowsCount = writePaymentsCsv(writer, reportQuery.getPayments(), zoneId);
                    } else if (reportQuery.isSetWithdrawals()) {
                        rowsCount = writeWithdrawalsCsv(writer, reportQuery.getWithdrawals(), zoneId);
                    } else {
                        throw new IllegalArgumentException("Stored report query must contain a known branch");
                    }
                    writer.flush();
                }
                return new GeneratedCsvReport(
                        fileName,
                        "text/csv",
                        stagedFile,
                        Files.size(stagedFile),
                        HexFormat.of().formatHex(md5.digest()),
                        HexFormat.of().formatHex(sha256.digest()),
                        rowsCount,
                        snapshotFixedAt
                );
            } catch (IOException ex) {
                deleteIfExists(stagedFile);
                throw new IllegalStateException("Failed to render CSV report", ex);
            } catch (RuntimeException ex) {
                deleteIfExists(stagedFile);
                throw ex;
            }
        }));
    }

    private long writePaymentsCsv(BufferedWriter writer, PaymentsQuery query, ZoneId zoneId) throws IOException {
        writer.write(String.join(",", PAYMENT_COLUMNS));
        writer.newLine();
        var sql = new StringBuilder(
                """
                        SELECT p.created_at AS created_at,
                               p.finalized_at AS finalized_at,
                               p.invoice_id AS invoice_id,
                               p.payment_id AS payment_id,
                               p.status AS status,
                               p.amount AS amount,
                               p.currency AS currency,
                               p.trx_id AS trx_id,
                               p.provider_id AS provider_id,
                               p.terminal_id AS terminal_id,
                               p.shop_id AS shop_id,
                               p.exchange_rate_internal AS exchange_rate_internal,
                               p.provider_amount AS provider_amount,
                               p.provider_currency AS provider_currency,
                               p.original_amount AS original_amount,
                               p.original_currency AS original_currency,
                               p.converted_amount AS converted_amount
                        FROM ccr.payment_txn_current p
                        LEFT JOIN ccr.shop_lookup sl ON sl.shop_id = p.shop_id
                        LEFT JOIN ccr.provider_lookup pl ON pl.provider_id = p.provider_id
                        LEFT JOIN ccr.terminal_lookup tl ON tl.terminal_id = p.terminal_id
                        WHERE p.created_at >= :fromTime
                          AND p.created_at < :toTime
                        """
        );
        var parameters =
                baseTimeRange(query.getTimeRange().getFromTime(), query.getTimeRange().getToTime());
        appendCommonFilters(sql, parameters, "p.party_id", "partyIds", query.getPartyIds());
        appendCommonFilters(sql, parameters, "p.shop_id", "shopIds", query.getShopIds());
        appendCommonFilters(sql, parameters, "p.provider_id", "providerIds", query.getProviderIds());
        appendCommonFilters(sql, parameters, "p.terminal_id", "terminalIds", query.getTerminalIds());
        appendCommonFilters(sql, parameters, "p.trx_id", "trxIds", query.getTrxIds());
        appendCommonFilters(sql, parameters, "p.currency", "currencies", query.getCurrencies());
        appendCommonFilters(sql, parameters, "p.status", "statuses", query.getStatuses());
        appendPaymentsSearchFilters(sql, parameters, query.getFilter());
        sql.append(" ORDER BY p.created_at ASC, p.invoice_id ASC, p.payment_id ASC");
        return writeRows(writer, sql.toString(), parameters, PAYMENT_COLUMNS, zoneId);
    }

    private long writeWithdrawalsCsv(
            BufferedWriter writer,
            WithdrawalsQuery query,
            ZoneId zoneId
    ) throws IOException {
        writer.write(String.join(",", WITHDRAWAL_COLUMNS));
        writer.newLine();
        var sql = new StringBuilder(
                """
                        SELECT w.created_at AS created_at,
                               w.finalized_at AS finalized_at,
                               w.withdrawal_id AS withdrawal_id,
                               w.status AS status,
                               w.amount AS amount,
                               w.currency AS currency,
                               w.trx_id AS trx_id,
                               w.provider_id AS provider_id,
                               w.terminal_id AS terminal_id,
                               w.wallet_id AS wallet_id,
                               w.exchange_rate_internal AS exchange_rate_internal,
                               w.provider_amount AS provider_amount,
                               w.provider_currency AS provider_currency,
                               w.original_amount AS original_amount,
                               w.original_currency AS original_currency,
                               w.converted_amount AS converted_amount
                        FROM ccr.withdrawal_txn_current w
                        LEFT JOIN ccr.wallet_lookup wl ON wl.wallet_id = w.wallet_id
                        LEFT JOIN ccr.provider_lookup pl ON pl.provider_id = w.provider_id
                        LEFT JOIN ccr.terminal_lookup tl ON tl.terminal_id = w.terminal_id
                        WHERE w.created_at >= :fromTime
                          AND w.created_at < :toTime
                        """
        );
        var parameters = baseTimeRange(
                query.getTimeRange().getFromTime(),
                query.getTimeRange().getToTime()
        );
        appendCommonFilters(sql, parameters, "w.party_id", "partyIds", query.getPartyIds());
        appendCommonFilters(sql, parameters, "w.wallet_id", "walletIds", query.getWalletIds());
        appendCommonFilters(sql, parameters, "w.provider_id", "providerIds", query.getProviderIds());
        appendCommonFilters(sql, parameters, "w.terminal_id", "terminalIds", query.getTerminalIds());
        appendCommonFilters(sql, parameters, "w.trx_id", "trxIds", query.getTrxIds());
        appendCommonFilters(sql, parameters, "w.currency", "currencies", query.getCurrencies());
        appendCommonFilters(sql, parameters, "w.status", "statuses", query.getStatuses());
        appendWithdrawalsSearchFilters(sql, parameters, query.getFilter());
        sql.append(" ORDER BY w.created_at ASC, w.withdrawal_id ASC");
        return writeRows(writer, sql.toString(), parameters, WITHDRAWAL_COLUMNS, zoneId);
    }

    private long writeRows(
            BufferedWriter writer,
            String sql,
            MapSqlParameterSource parameters,
            List<String> columns,
            ZoneId zoneId
    ) {
        var rowCount = new long[] {0L};
        try {
            var preparedStatementCreator = buildCursorPreparedStatement(sql, parameters);
            jdbcTemplate.getJdbcTemplate().query(preparedStatementCreator, resultSet -> {
                try {
                    writeRow(writer, resultSet, columns, zoneId);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
                rowCount[0]++;
            });
        } catch (UncheckedIOException ex) {
            throw new IllegalStateException("Failed to write CSV rows", ex);
        }
        return rowCount[0];
    }

    private PreparedStatementCreator buildCursorPreparedStatement(String sql, MapSqlParameterSource parameters) {
        var parsedSql = NamedParameterUtils.parseSqlStatement(sql);
        var expandedSql = NamedParameterUtils.substituteNamedParameters(parsedSql, parameters);
        var sqlParameters = NamedParameterUtils.buildSqlParameterList(parsedSql, parameters);
        var values = NamedParameterUtils.buildValueArray(parsedSql, parameters, null);
        var preparedStatementCreatorFactory =
                new PreparedStatementCreatorFactory(expandedSql, sqlParameters);
        var preparedStatementSetter =
                preparedStatementCreatorFactory.newPreparedStatementSetter(values);
        return connection -> prepareCursorStatement(connection, expandedSql, preparedStatementSetter);
    }

    private PreparedStatement prepareCursorStatement(
            Connection connection,
            String sql,
            PreparedStatementSetter preparedStatementSetter
    ) throws SQLException {
        var preparedStatement = connection.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY
        );
        preparedStatement.setFetchSize(CSV_QUERY_FETCH_SIZE);
        preparedStatementSetter.setValues(preparedStatement);
        return preparedStatement;
    }

    private void writeRow(
            BufferedWriter writer,
            ResultSet resultSet,
            List<String> columns,
            ZoneId zoneId
    ) throws SQLException, IOException {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                writer.write(',');
            }
            var column = columns.get(i);
            writer.write(escapeCsv(renderValue(resultSet, column, zoneId)));
        }
        writer.newLine();
    }

    private String renderValue(ResultSet resultSet, String column, ZoneId zoneId) throws SQLException {
        return switch (column) {
            case "created_date" -> renderTimestampDate(resultSet.getTimestamp("created_at"), zoneId);
            case "created_time" -> renderTimestampTime(resultSet.getTimestamp("created_at"), zoneId);
            case "finalized_date" -> renderTimestampDate(resultSet.getTimestamp("finalized_at"), zoneId);
            case "finalized_time" -> renderTimestampTime(resultSet.getTimestamp("finalized_at"), zoneId);
            case "amount" -> renderMinorUnits(resultSet.getObject("amount"), resultSet.getString("currency"));
            case "provider_amount" -> renderMinorUnits(
                    resultSet.getObject("provider_amount"),
                    firstNonBlank(resultSet.getString("provider_currency"), resultSet.getString("currency"))
            );
            case "original_amount" -> renderMinorUnits(
                    resultSet.getObject("original_amount"),
                    resultSet.getString("original_currency")
            );
            case "converted_amount" -> renderMinorUnits(
                    resultSet.getObject("converted_amount"),
                    resultSet.getString("currency")
            );
            default -> renderScalarValue(resultSet.getObject(column));
        };
    }

    private String renderScalarValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof BigDecimal bigDecimal) {
            return bigDecimal.toPlainString();
        }
        return value.toString();
    }

    private String renderTimestampDate(Timestamp timestamp, ZoneId zoneId) {
        if (timestamp == null) {
            return "";
        }
        var localDateTime = timestamp.toInstant().atZone(zoneId).toLocalDateTime();
        return CSV_DATE_FORMATTER.format(localDateTime.toLocalDate());
    }

    private String renderTimestampTime(Timestamp timestamp, ZoneId zoneId) {
        if (timestamp == null) {
            return "";
        }
        var localDateTime = timestamp.toInstant().atZone(zoneId).toLocalDateTime();
        return CSV_TIME_FORMATTER.format(localDateTime.toLocalTime());
    }

    private String renderMinorUnits(Object value, String currencyCode) {
        if (value == null) {
            return "";
        }
        if (!(value instanceof Number number)) {
            throw new IllegalStateException("Expected numeric minor units for currency-formatted CSV column");
        }
        if (currencyCode == null || currencyCode.isBlank()) {
            return Long.toString(number.longValue());
        }
        var exponent = currencyExponent(currencyCode);
        return BigDecimal.valueOf(number.longValue(), exponent).toPlainString();
    }

    private int currencyExponent(String currencyCode) {
        try {
            var currency = Currency.getInstance(currencyCode.toUpperCase(Locale.ROOT));
            var exponent = currency.getDefaultFractionDigits();
            if (exponent < 0) {
                throw new IllegalStateException("Unsupported currency exponent for " + currencyCode);
            }
            return exponent;
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException("Unknown currency code for CSV formatting: " + currencyCode, ex);
        }
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        return second;
    }

    private String escapeCsv(String value) {
        if (!value.contains(",") && !value.contains("\"") && !value.contains("\n")) {
            return value;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private MapSqlParameterSource baseTimeRange(String fromTime, String toTime) {
        return new MapSqlParameterSource()
                .addValue("fromTime", Timestamp.from(Instant.parse(fromTime)))
                .addValue("toTime", Timestamp.from(Instant.parse(toTime)));
    }

    private void appendCommonFilters(
            StringBuilder sql,
            MapSqlParameterSource parameters,
            String column,
            String parameterName,
            List<String> values
    ) {
        if (values == null || values.isEmpty()) {
            return;
        }
        sql.append(" AND ").append(column).append(" IN (:").append(parameterName).append(")");
        parameters.addValue(parameterName, values);
    }

    private void appendPaymentsSearchFilters(
            StringBuilder sql,
            MapSqlParameterSource parameters,
            PaymentsSearchFilter filter
    ) {
        if (filter == null) {
            return;
        }
        appendSearchFilter(
                sql,
                parameters,
                "lower(coalesce(sl.shop_search, ''))",
                "shopTerm",
                filter.getShopTerm()
        );
        appendSearchFilter(
                sql,
                parameters,
                "lower(coalesce(pl.provider_search, ''))",
                "providerTerm",
                filter.getProviderTerm()
        );
        appendSearchFilter(
                sql,
                parameters,
                "lower(coalesce(tl.terminal_search, ''))",
                "terminalTerm",
                filter.getTerminalTerm()
        );
        appendSearchFilter(sql, parameters, "lower(coalesce(p.trx_search, ''))", "trxTerm", filter.getTrxTerm());
    }

    private void appendWithdrawalsSearchFilters(
            StringBuilder sql,
            MapSqlParameterSource parameters,
            WithdrawalsSearchFilter filter
    ) {
        if (filter == null) {
            return;
        }
        appendSearchFilter(
                sql,
                parameters,
                "lower(coalesce(wl.wallet_search, ''))",
                "walletTerm",
                filter.getWalletTerm()
        );
        appendSearchFilter(
                sql,
                parameters,
                "lower(coalesce(pl.provider_search, ''))",
                "providerTerm",
                filter.getProviderTerm()
        );
        appendSearchFilter(
                sql,
                parameters,
                "lower(coalesce(tl.terminal_search, ''))",
                "terminalTerm",
                filter.getTerminalTerm()
        );
        appendSearchFilter(sql, parameters, "lower(coalesce(w.trx_search, ''))", "trxTerm", filter.getTrxTerm());
    }

    private void appendSearchFilter(
            StringBuilder sql,
            MapSqlParameterSource parameters,
            String column,
            String parameterName,
            String value
    ) {
        if (value == null || value.isBlank()) {
            return;
        }
        sql.append(" AND ").append(column).append(" LIKE :").append(parameterName);
        parameters.addValue(parameterName, "%" + value.toLowerCase() + "%");
    }

    private Instant currentSnapshot() {
        var epochSeconds = Objects.requireNonNull(
                jdbcTemplate.getJdbcTemplate().queryForObject(
                        "SELECT EXTRACT(EPOCH FROM now())",
                        BigDecimal.class
                ),
                "Current snapshot query must return epoch seconds"
        );
        var seconds = epochSeconds.longValue();
        var nanos = epochSeconds.subtract(BigDecimal.valueOf(seconds))
                .movePointRight(9)
                .longValue();
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private Path createTempFile(long reportId) {
        try {
            return Files.createTempFile("ccr-report-" + reportId + "-", ".csv");
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to allocate temp file for report " + reportId, ex);
        }
    }

    private MessageDigest createDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to initialize " + algorithm + " digest", ex);
        }
    }

    private void deleteIfExists(Path stagedFile) {
        try {
            Files.deleteIfExists(stagedFile);
        } catch (IOException ignored) {
            // Best-effort cleanup for abandoned staged files.
        }
    }
}
