package dev.vality.ccreporter.report;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.dao.ClaimedReportJob;
import dev.vality.ccreporter.util.ThriftQueryCodec;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class ReportCsvService {

    private static final DateTimeFormatter CSV_TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private static final List<String> PAYMENT_COLUMNS = List.of(
            "invoice_id",
            "payment_id",
            "party_id",
            "shop_id",
            "shop_name",
            "created_at",
            "finalized_at",
            "status",
            "provider_id",
            "provider_name",
            "terminal_id",
            "terminal_name",
            "amount",
            "fee",
            "currency",
            "trx_id",
            "external_id",
            "rrn",
            "approval_code",
            "payment_tool_type",
            "original_amount",
            "original_currency",
            "converted_amount",
            "exchange_rate_internal",
            "provider_amount",
            "provider_currency"
    );

    private static final List<String> WITHDRAWAL_COLUMNS = List.of(
            "withdrawal_id",
            "party_id",
            "wallet_id",
            "wallet_name",
            "destination_id",
            "created_at",
            "finalized_at",
            "status",
            "provider_id",
            "provider_name",
            "terminal_id",
            "terminal_name",
            "amount",
            "fee",
            "currency",
            "trx_id",
            "external_id",
            "error_code",
            "error_reason",
            "error_sub_failure",
            "original_amount",
            "original_currency",
            "converted_amount",
            "exchange_rate_internal",
            "provider_amount",
            "provider_currency"
    );

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ThriftQueryCodec thriftQueryCodec;
    private final TransactionTemplate readOnlyRepeatableReadTx;

    public ReportCsvService(
            NamedParameterJdbcTemplate jdbcTemplate,
            ThriftQueryCodec thriftQueryCodec,
            PlatformTransactionManager transactionManager
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.thriftQueryCodec = thriftQueryCodec;
        this.readOnlyRepeatableReadTx = new TransactionTemplate(transactionManager);
        this.readOnlyRepeatableReadTx.setReadOnly(true);
        this.readOnlyRepeatableReadTx.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }

    public GeneratedCsvReport generate(ClaimedReportJob claimedReportJob) {
        return Objects.requireNonNull(readOnlyRepeatableReadTx.execute(status -> {
            Instant snapshotFixedAt = currentSnapshot();
            ReportQuery reportQuery = thriftQueryCodec.deserialize(claimedReportJob.queryJson());
            ZoneId zoneId = ZoneId.of(claimedReportJob.timezone());
            String fileName = claimedReportJob.reportType().name() + "-report-" + claimedReportJob.id() + ".csv";
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
                );
                long rowsCount;
                if (reportQuery.isSetPayments()) {
                    rowsCount = writePaymentsCsv(writer, reportQuery.getPayments(), zoneId);
                } else if (reportQuery.isSetWithdrawals()) {
                    rowsCount = writeWithdrawalsCsv(writer, reportQuery.getWithdrawals(), zoneId);
                } else {
                    throw new IllegalArgumentException("Stored report query must contain a known branch");
                }
                writer.flush();
                return new GeneratedCsvReport(
                        fileName,
                        "text/csv",
                        outputStream.toByteArray(),
                        rowsCount,
                        snapshotFixedAt
                );
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to render CSV report", ex);
            }
        }));
    }

    private long writePaymentsCsv(BufferedWriter writer, PaymentsQuery query, ZoneId zoneId) throws IOException {
        writer.write(String.join(",", PAYMENT_COLUMNS));
        writer.newLine();
        StringBuilder sql = new StringBuilder(
                """
                        SELECT invoice_id, payment_id, party_id, shop_id, shop_name, created_at, finalized_at, status,
                               provider_id, provider_name, terminal_id, terminal_name, amount, fee, currency, trx_id,
                               external_id, rrn, approval_code, payment_tool_type, original_amount, original_currency,
                               converted_amount, exchange_rate_internal, provider_amount, provider_currency
                        FROM ccr.payment_txn_current
                        WHERE created_at >= :fromTime
                          AND created_at < :toTime
                        """
        );
        MapSqlParameterSource parameters =
                baseTimeRange(query.getTimeRange().getFromTime(), query.getTimeRange().getToTime());
        appendCommonFilters(sql, parameters, "party_id", "partyIds", query.getPartyIds());
        appendCommonFilters(sql, parameters, "shop_id", "shopIds", query.getShopIds());
        appendCommonFilters(sql, parameters, "provider_id", "providerIds", query.getProviderIds());
        appendCommonFilters(sql, parameters, "terminal_id", "terminalIds", query.getTerminalIds());
        appendCommonFilters(sql, parameters, "trx_id", "trxIds", query.getTrxIds());
        appendCommonFilters(sql, parameters, "currency", "currencies", query.getCurrencies());
        appendCommonFilters(sql, parameters, "status", "statuses", query.getStatuses());
        appendPaymentsSearchFilters(sql, parameters, query.getFilter());
        sql.append(" ORDER BY created_at ASC, invoice_id ASC, payment_id ASC");
        return writeRows(writer, sql.toString(), parameters, PAYMENT_COLUMNS, zoneId);
    }

    private long writeWithdrawalsCsv(
            BufferedWriter writer,
            WithdrawalsQuery query,
            ZoneId zoneId
    ) throws IOException {
        writer.write(String.join(",", WITHDRAWAL_COLUMNS));
        writer.newLine();
        StringBuilder sql = new StringBuilder(
                """
                        SELECT withdrawal_id, party_id, wallet_id, wallet_name, destination_id,
                               created_at, finalized_at,
                               status, provider_id, provider_name, terminal_id, terminal_name, amount, fee, currency,
                               trx_id, external_id, error_code, error_reason, error_sub_failure, original_amount,
                               original_currency, converted_amount, exchange_rate_internal, provider_amount,
                               provider_currency
                        FROM ccr.withdrawal_txn_current
                        WHERE created_at >= :fromTime
                          AND created_at < :toTime
                        """
        );
        MapSqlParameterSource parameters = baseTimeRange(
                query.getTimeRange().getFromTime(),
                query.getTimeRange().getToTime()
        );
        appendCommonFilters(sql, parameters, "party_id", "partyIds", query.getPartyIds());
        appendCommonFilters(sql, parameters, "wallet_id", "walletIds", query.getWalletIds());
        appendCommonFilters(sql, parameters, "provider_id", "providerIds", query.getProviderIds());
        appendCommonFilters(sql, parameters, "terminal_id", "terminalIds", query.getTerminalIds());
        appendCommonFilters(sql, parameters, "trx_id", "trxIds", query.getTrxIds());
        appendCommonFilters(sql, parameters, "currency", "currencies", query.getCurrencies());
        appendCommonFilters(sql, parameters, "status", "statuses", query.getStatuses());
        appendWithdrawalsSearchFilters(sql, parameters, query.getFilter());
        sql.append(" ORDER BY created_at ASC, withdrawal_id ASC");
        return writeRows(writer, sql.toString(), parameters, WITHDRAWAL_COLUMNS, zoneId);
    }

    private long writeRows(
            BufferedWriter writer,
            String sql,
            MapSqlParameterSource parameters,
            List<String> columns,
            ZoneId zoneId
    ) {
        List<List<String>> rows =
                jdbcTemplate.query(sql, parameters, (resultSet, rowNum) -> mapRow(resultSet, columns, zoneId));
        try {
            for (List<String> row : rows) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to write CSV rows", ex);
        }
        return rows.size();
    }

    private List<String> mapRow(ResultSet resultSet, List<String> columns, ZoneId zoneId) throws SQLException {
        List<String> row = new ArrayList<>(columns.size());
        for (String column : columns) {
            row.add(escapeCsv(renderValue(resultSet.getObject(column), zoneId)));
        }
        return row;
    }

    private String renderValue(Object value, ZoneId zoneId) {
        if (value == null) {
            return "";
        }
        if (value instanceof Timestamp timestamp) {
            return CSV_TIMESTAMP_FORMATTER.format(timestamp.toInstant().atZone(zoneId));
        }
        if (value instanceof BigDecimal bigDecimal) {
            return bigDecimal.toPlainString();
        }
        return value.toString();
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
        appendSearchFilter(sql, parameters, "shop_search", "shopTerm", filter.getShopTerm());
        appendSearchFilter(sql, parameters, "provider_search", "providerTerm", filter.getProviderTerm());
        appendSearchFilter(sql, parameters, "terminal_search", "terminalTerm", filter.getTerminalTerm());
        appendSearchFilter(sql, parameters, "trx_search", "trxTerm", filter.getTrxTerm());
    }

    private void appendWithdrawalsSearchFilters(
            StringBuilder sql,
            MapSqlParameterSource parameters,
            WithdrawalsSearchFilter filter
    ) {
        if (filter == null) {
            return;
        }
        appendSearchFilter(sql, parameters, "wallet_search", "walletTerm", filter.getWalletTerm());
        appendSearchFilter(sql, parameters, "provider_search", "providerTerm", filter.getProviderTerm());
        appendSearchFilter(sql, parameters, "terminal_search", "terminalTerm", filter.getTerminalTerm());
        appendSearchFilter(sql, parameters, "trx_search", "trxTerm", filter.getTrxTerm());
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
        sql.append(" AND lower(coalesce(").append(column).append(", '')) LIKE :").append(parameterName);
        parameters.addValue(parameterName, "%" + value.toLowerCase() + "%");
    }

    private Instant currentSnapshot() {
        BigDecimal epochSeconds = jdbcTemplate.getJdbcTemplate().queryForObject(
                "SELECT EXTRACT(EPOCH FROM now())",
                BigDecimal.class
        );
        long seconds = epochSeconds.longValue();
        long nanos = epochSeconds.subtract(BigDecimal.valueOf(seconds))
                .movePointRight(9)
                .longValue();
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
