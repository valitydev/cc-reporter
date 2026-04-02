package dev.vality.ccreporter.report;

import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.WithdrawalsQuery;
import dev.vality.ccreporter.dao.ReportCsvDao;
import dev.vality.ccreporter.dao.mapper.ReportRecordMapper;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;
import dev.vality.ccreporter.model.GeneratedCsvReport;
import dev.vality.ccreporter.serde.json.ThriftJsonCodec;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.jooq.Cursor;
import org.jooq.Record;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Currency;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class ReportCsvService {

    private static final DateTimeFormatter CSV_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter CSV_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
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
            "original_currency"
    );

    private final ReportCsvDao reportCsvDao;
    private final ThriftJsonCodec thriftJsonCodec;

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public GeneratedCsvReport generate(ReportJob reportJob) {
        var snapshotFixedAt = reportCsvDao.currentSnapshot();
        var reportQuery = thriftJsonCodec.deserialize(reportJob.getQueryJson().data(), ReportQuery.class);
        var zoneId = ZoneId.of(reportJob.getTimezone());
        var reportType = ReportRecordMapper.mapEnum(reportJob.getReportType(), ReportType.class);
        var fileName = reportType.name() + "-report-" + reportJob.getId() + ".csv";
        var stagedFile = createTempFile(reportJob.getId());
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
                rowsCount = switch (reportType) {
                    case payments -> writePaymentsCsv(writer, reportQuery.getPayments(), zoneId);
                    case withdrawals -> writeWithdrawalsCsv(writer, reportQuery.getWithdrawals(), zoneId);
                };
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
    }

    private long writePaymentsCsv(BufferedWriter writer, PaymentsQuery query, ZoneId zoneId) throws IOException {
        writer.write(String.join(",", PAYMENT_COLUMNS));
        writer.newLine();
        try (var rows = reportCsvDao.fetchPayments(query)) {
            return writeRows(writer, rows, PAYMENT_COLUMNS, zoneId);
        }
    }

    private long writeWithdrawalsCsv(
            BufferedWriter writer,
            WithdrawalsQuery query,
            ZoneId zoneId
    ) throws IOException {
        writer.write(String.join(",", WITHDRAWAL_COLUMNS));
        writer.newLine();
        try (var rows = reportCsvDao.fetchWithdrawals(query)) {
            return writeRows(writer, rows, WITHDRAWAL_COLUMNS, zoneId);
        }
    }

    @SneakyThrows
    private long writeRows(
            BufferedWriter writer,
            Cursor<? extends Record> rows,
            List<String> columns,
            ZoneId zoneId
    ) {
        var rowCount = 0L;
        for (var row : rows) {
            writeRow(writer, row, columns, zoneId);
            rowCount++;
        }
        return rowCount;
    }

    private void writeRow(
            BufferedWriter writer,
            Record row,
            List<String> columns,
            ZoneId zoneId
    ) throws IOException {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                writer.write(',');
            }
            var column = columns.get(i);
            writer.write(escapeCsv(renderValue(row, column, zoneId)));
        }
        writer.newLine();
    }

    private String renderValue(Record row, String column, ZoneId zoneId) {
        return switch (column) {
            case "created_date" -> renderTimestampDate(row.get("created_at", Timestamp.class), zoneId);
            case "created_time" -> renderTimestampTime(row.get("created_at", Timestamp.class), zoneId);
            case "finalized_date" -> renderTimestampDate(row.get("finalized_at", Timestamp.class), zoneId);
            case "finalized_time" -> renderTimestampTime(row.get("finalized_at", Timestamp.class), zoneId);
            case "amount" -> renderMinorUnits(row.get("amount"), row.get("currency", String.class));
            case "provider_amount" -> renderMinorUnits(
                    row.get("provider_amount"),
                    firstNonBlank(row.get("provider_currency", String.class), row.get("currency", String.class))
            );
            case "original_amount" -> renderMinorUnits(
                    row.get("original_amount"),
                    row.get("original_currency", String.class)
            );
            case "converted_amount" -> renderMinorUnits(
                    row.get("converted_amount"),
                    row.get("currency", String.class)
            );
            default -> renderScalarValue(row.get(column));
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
