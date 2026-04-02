package dev.vality.ccreporter.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.dao.ReportCsvDao;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.serde.json.ThriftJsonCodec;
import org.jooq.Cursor;
import org.jooq.Record;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ReportCsvServiceTest {

    @Test
    void paymentsCsvIsRenderedFromDaoCursor() throws Exception {
        var reportCsvDao = mock(ReportCsvDao.class);
        var objectMapper = new ObjectMapper();
        var thriftJsonCodec = new ThriftJsonCodec(objectMapper);
        var reportCsvService = new ReportCsvService(reportCsvDao, thriftJsonCodec);
        @SuppressWarnings("unchecked")
        var cursor = mock(Cursor.class);
        var row = mock(Record.class);

        when(reportCsvDao.currentSnapshot()).thenReturn(Instant.parse("2023-11-14T22:13:20.123456789Z"));
        when(reportCsvDao.fetchPayments(any())).thenReturn(cursor);
        when(cursor.iterator()).thenReturn(List.of(row).iterator());
        when(row.get("created_at", Timestamp.class)).thenReturn(Timestamp.from(Instant.parse("2026-01-01T10:00:00Z")));
        when(row.get("finalized_at", Timestamp.class))
                .thenReturn(Timestamp.from(Instant.parse("2026-01-01T11:00:00Z")));
        when(row.get("amount")).thenReturn(1000L);
        when(row.get("provider_amount")).thenReturn(990L);
        when(row.get("original_amount")).thenReturn(1100L);
        when(row.get("converted_amount")).thenReturn(1000L);
        when(row.get("invoice_id")).thenReturn("invoice-cursor-1");
        when(row.get("payment_id")).thenReturn("payment-cursor-1");
        when(row.get("status")).thenReturn("captured");
        when(row.get("trx_id")).thenReturn("trx-cursor-1");
        when(row.get("provider_id")).thenReturn("provider-1");
        when(row.get("terminal_id")).thenReturn("terminal-1");
        when(row.get("shop_id")).thenReturn("shop-1");
        when(row.get("exchange_rate_internal")).thenReturn(new BigDecimal("1.1000000000"));
        when(row.get("provider_currency")).thenReturn("EUR");
        when(row.get("original_currency")).thenReturn("USD");
        when(row.get("currency")).thenReturn("RUB");
        when(row.get("currency", String.class)).thenReturn("RUB");
        when(row.get("provider_currency", String.class)).thenReturn("EUR");
        when(row.get("original_currency", String.class)).thenReturn("USD");

        var generatedCsvReport = reportCsvService.generate(claimedPaymentsJob(thriftJsonCodec));

        verify(reportCsvDao).fetchPayments(any());
        verify(cursor).close();
        assertThat(generatedCsvReport.rowsCount()).isEqualTo(1L);
        assertThat(generatedCsvReport.dataSnapshotFixedAt())
                .isEqualTo(Instant.parse("2023-11-14T22:13:20.123456789Z"));
        assertThat(Files.readString(generatedCsvReport.contentPath(), StandardCharsets.UTF_8))
                .contains("invoice-cursor-1,payment-cursor-1,captured,10.00,RUB,trx-cursor-1");

        Files.deleteIfExists(generatedCsvReport.contentPath());
    }

    private ReportJob claimedPaymentsJob(ThriftJsonCodec thriftJsonCodec) {
        var request = ReportRequestFixtures.payments(
                "cursor-fetch-size-1",
                new TimeRange("2025-12-31T00:00:00Z", "2026-01-02T00:00:00Z")
        );
        request.setTimezone("Asia/Krasnoyarsk");
        var reportQuery = request.getQuery();
        return new ReportJob()
                .setId(42L)
                .setReportType(dev.vality.ccreporter.domain.enums.ReportType.payments)
                .setFileType(dev.vality.ccreporter.domain.enums.FileType.csv)
                .setQueryJson(org.jooq.JSONB.valueOf(thriftJsonCodec.serialize(reportQuery)))
                .setTimezone(request.getTimezone())
                .setAttempt(1);
    }
}
