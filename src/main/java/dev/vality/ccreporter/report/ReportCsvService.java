package dev.vality.ccreporter.report;

import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.WithdrawalsQuery;
import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.dao.ReportCsvDao;
import dev.vality.ccreporter.model.GeneratedCsvReport;
import dev.vality.ccreporter.model.ReportTask;
import dev.vality.ccreporter.serde.json.ThriftJsonCodec;
import lombok.RequiredArgsConstructor;
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
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Currency;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CancellationException;

@Service
@RequiredArgsConstructor
public class ReportCsvService {

    private static final String CSV_LINE_ENDING = "\r\n";
    private static final DateTimeFormatter CSV_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter CSV_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final String CREATED_DATE_COLUMN = "created_date";
    private static final String CREATED_TIME_COLUMN = "created_time";
    private static final String FINALIZED_DATE_COLUMN = "finalized_date";
    private static final String FINALIZED_TIME_COLUMN = "finalized_time";
    private static final String INVOICE_ID_COLUMN = "invoice_id";
    private static final String PAYMENT_ID_COLUMN = "payment_id";
    private static final String WITHDRAWAL_ID_COLUMN = "withdrawal_id";
    private static final String STATUS_COLUMN = "status";
    private static final String AMOUNT_COLUMN = "amount";
    private static final String CURRENCY_COLUMN = "currency";
    private static final String TRX_ID_COLUMN = "trx_id";
    private static final String PROVIDER_ID_COLUMN = "provider_id";
    private static final String TERMINAL_ID_COLUMN = "terminal_id";
    private static final String SHOP_ID_COLUMN = "shop_id";
    private static final String WALLET_ID_COLUMN = "wallet_id";
    private static final String EXCHANGE_RATE_INTERNAL_COLUMN = "exchange_rate_internal";
    private static final String PROVIDER_AMOUNT_COLUMN = "provider_amount";
    private static final String PROVIDER_CURRENCY_COLUMN = "provider_currency";
    private static final String ORIGINAL_AMOUNT_COLUMN = "original_amount";
    private static final String ORIGINAL_CURRENCY_COLUMN = "original_currency";
    private static final String CONVERTED_AMOUNT_COLUMN = "converted_amount";

    private static final List<String> PAYMENT_COLUMNS = List.of(
            CREATED_DATE_COLUMN,
            CREATED_TIME_COLUMN,
            FINALIZED_DATE_COLUMN,
            FINALIZED_TIME_COLUMN,
            INVOICE_ID_COLUMN,
            PAYMENT_ID_COLUMN,
            STATUS_COLUMN,
            AMOUNT_COLUMN,
            CURRENCY_COLUMN,
            TRX_ID_COLUMN,
            PROVIDER_ID_COLUMN,
            TERMINAL_ID_COLUMN,
            SHOP_ID_COLUMN,
            EXCHANGE_RATE_INTERNAL_COLUMN,
            PROVIDER_AMOUNT_COLUMN,
            PROVIDER_CURRENCY_COLUMN,
            ORIGINAL_AMOUNT_COLUMN,
            ORIGINAL_CURRENCY_COLUMN,
            CONVERTED_AMOUNT_COLUMN
    );

    private static final List<String> WITHDRAWAL_COLUMNS = List.of(
            CREATED_DATE_COLUMN,
            CREATED_TIME_COLUMN,
            FINALIZED_DATE_COLUMN,
            FINALIZED_TIME_COLUMN,
            WITHDRAWAL_ID_COLUMN,
            STATUS_COLUMN,
            AMOUNT_COLUMN,
            CURRENCY_COLUMN,
            TRX_ID_COLUMN,
            PROVIDER_ID_COLUMN,
            TERMINAL_ID_COLUMN,
            WALLET_ID_COLUMN,
            EXCHANGE_RATE_INTERNAL_COLUMN,
            PROVIDER_AMOUNT_COLUMN,
            PROVIDER_CURRENCY_COLUMN,
            ORIGINAL_AMOUNT_COLUMN,
            ORIGINAL_CURRENCY_COLUMN,
            CONVERTED_AMOUNT_COLUMN
    );

    private final ReportCsvDao reportCsvDao;
    private final ThriftJsonCodec thriftJsonCodec;
    private final ReportProperties reportProperties;

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public GeneratedCsvReport generate(ReportTask reportTask) {
        reportCsvDao.setLocalStatementTimeout(reportProperties.getProcessingTimeoutMs());
        var snapshotFixedAt = reportCsvDao.currentSnapshot();
        var reportQuery = thriftJsonCodec.deserialize(reportTask.queryJson(), ReportQuery.class);
        var zoneId = ZoneId.of(reportTask.timezone());
        var reportType = reportTask.reportType();
        var fileName = reportType.name() + "-report-" + reportTask.id() + ".csv";
        var stagedFile = createTempFile(reportTask.id());
        try {
            var md5 = createDigest("MD5");
            var sha256 = createDigest("SHA-256");
            long rowsCount;
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
        writer.write(CSV_LINE_ENDING);
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
        writer.write(CSV_LINE_ENDING);
        try (var rows = reportCsvDao.fetchWithdrawals(query)) {
            return writeRows(writer, rows, WITHDRAWAL_COLUMNS, zoneId);
        }
    }

    private long writeRows(
            BufferedWriter writer,
            Cursor<? extends Record> rows,
            List<String> columns,
            ZoneId zoneId
    ) throws IOException {
        var rowCount = 0L;
        for (var row : rows) {
            throwIfInterrupted();
            writeRow(writer, row, columns, zoneId);
            rowCount++;
        }
        return rowCount;
    }

    private void throwIfInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Report CSV generation was interrupted");
        }
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
        writer.write(CSV_LINE_ENDING);
    }

    private String renderValue(Record row, String column, ZoneId zoneId) {
        return switch (column) {
            case CREATED_DATE_COLUMN -> renderTimestampDate(row.get("created_at", LocalDateTime.class), zoneId);
            case CREATED_TIME_COLUMN -> renderTimestampTime(row.get("created_at", LocalDateTime.class), zoneId);
            case FINALIZED_DATE_COLUMN -> renderTimestampDate(row.get("finalized_at", LocalDateTime.class), zoneId);
            case FINALIZED_TIME_COLUMN -> renderTimestampTime(row.get("finalized_at", LocalDateTime.class), zoneId);
            case AMOUNT_COLUMN -> renderMinorUnits(row.get("amount"), row.get("currency", String.class));
            case PROVIDER_AMOUNT_COLUMN -> renderMinorUnits(
                    row.get("provider_amount"),
                    firstNonBlank(row.get("provider_currency", String.class), row.get("currency", String.class))
            );
            case ORIGINAL_AMOUNT_COLUMN -> renderMinorUnits(
                    row.get("original_amount"),
                    row.get("original_currency", String.class)
            );
            case CONVERTED_AMOUNT_COLUMN -> renderMinorUnits(
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

    private String renderTimestampDate(LocalDateTime timestamp, ZoneId zoneId) {
        if (timestamp == null) {
            return "";
        }
        var localDateTime = timestamp.atZone(ZoneOffset.UTC).withZoneSameInstant(zoneId).toLocalDateTime();
        return CSV_DATE_FORMATTER.format(localDateTime.toLocalDate());
    }

    private String renderTimestampTime(LocalDateTime timestamp, ZoneId zoneId) {
        if (timestamp == null) {
            return "";
        }
        var localDateTime = timestamp.atZone(ZoneOffset.UTC).withZoneSameInstant(zoneId).toLocalDateTime();
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
        if (!value.contains(",")
                && !value.contains("\"")
                && !value.contains("\n")
                && !value.contains("\r")) {
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
        } catch (NoSuchAlgorithmException ex) {
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
