package dev.vality.ccreporter.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.model.ClaimedReportJob;
import dev.vality.ccreporter.serde.json.ThriftJsonCodec;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ReportCsvServiceTest {

    @Test
    void paymentsCsvQueryUsesForwardOnlyCursorFetchSize() throws Exception {
        var namedParameterJdbcTemplate = mock(NamedParameterJdbcTemplate.class);
        var jdbcTemplate = mock(JdbcTemplate.class);
        var objectMapper = new ObjectMapper();
        var thriftJsonCodec = new ThriftJsonCodec(objectMapper);
        var reportCsvService = new ReportCsvService(namedParameterJdbcTemplate, thriftJsonCodec);

        when(namedParameterJdbcTemplate.getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(jdbcTemplate.queryForObject("SELECT EXTRACT(EPOCH FROM now())", BigDecimal.class))
                .thenReturn(new BigDecimal("1700000000.123456789"));

        var connection = mock(Connection.class);
        var preparedStatement = mock(PreparedStatement.class);
        try (var resultSet = mock(ResultSet.class)) {
            when(connection.prepareStatement(
                    any(String.class),
                    eq(ResultSet.TYPE_FORWARD_ONLY),
                    eq(ResultSet.CONCUR_READ_ONLY)
            ))
                    .thenReturn(preparedStatement);

            doAnswer(invocation -> {
                var preparedStatementCreator = invocation.<PreparedStatementCreator>getArgument(0);
                preparedStatementCreator.createPreparedStatement(connection);

                when(resultSet.getTimestamp("created_at"))
                        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T10:00:00Z")));
                when(resultSet.getTimestamp("finalized_at"))
                        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T11:00:00Z")));
                when(resultSet.getObject("amount")).thenReturn(1000L);
                when(resultSet.getObject("provider_amount")).thenReturn(990L);
                when(resultSet.getObject("original_amount")).thenReturn(1100L);
                when(resultSet.getObject("converted_amount")).thenReturn(1000L);
                when(resultSet.getObject("invoice_id")).thenReturn("invoice-cursor-1");
                when(resultSet.getObject("payment_id")).thenReturn("payment-cursor-1");
                when(resultSet.getObject("status")).thenReturn("captured");
                when(resultSet.getObject("trx_id")).thenReturn("trx-cursor-1");
                when(resultSet.getObject("provider_id")).thenReturn("provider-1");
                when(resultSet.getObject("terminal_id")).thenReturn("terminal-1");
                when(resultSet.getObject("shop_id")).thenReturn("shop-1");
                when(resultSet.getObject("exchange_rate_internal")).thenReturn(new BigDecimal("1.1000000000"));
                when(resultSet.getObject("provider_currency")).thenReturn("EUR");
                when(resultSet.getObject("original_currency")).thenReturn("USD");
                when(resultSet.getObject("currency")).thenReturn("RUB");
                when(resultSet.getString("currency")).thenReturn("RUB");
                when(resultSet.getString("provider_currency")).thenReturn("EUR");
                when(resultSet.getString("original_currency")).thenReturn("USD");

                var rowCallbackHandler = invocation.<RowCallbackHandler>getArgument(1);
                rowCallbackHandler.processRow(resultSet);
                return null;
            }).when(jdbcTemplate).query(any(PreparedStatementCreator.class), any(RowCallbackHandler.class));

            var generatedCsvReport = reportCsvService.generate(claimedPaymentsJob(thriftJsonCodec));

            verify(connection).prepareStatement(
                    any(String.class),
                    eq(ResultSet.TYPE_FORWARD_ONLY),
                    eq(ResultSet.CONCUR_READ_ONLY)
            );
            verify(preparedStatement).setFetchSize(1_000);
            assertThat(generatedCsvReport.rowsCount()).isEqualTo(1L);
            assertThat(generatedCsvReport.dataSnapshotFixedAt())
                    .isEqualTo(Instant.parse("2023-11-14T22:13:20.123456789Z"));
            assertThat(Files.readString(generatedCsvReport.contentPath(), StandardCharsets.UTF_8))
                    .contains("invoice-cursor-1,payment-cursor-1,captured,10.00,RUB,trx-cursor-1");

            Files.deleteIfExists(generatedCsvReport.contentPath());
        }
    }

    private ClaimedReportJob claimedPaymentsJob(ThriftJsonCodec thriftJsonCodec) {
        var request = ReportRequestFixtures.payments(
                "cursor-fetch-size-1",
                new TimeRange("2025-12-31T00:00:00Z", "2026-01-02T00:00:00Z")
        );
        request.setTimezone("Asia/Krasnoyarsk");
        var reportQuery = request.getQuery();
        return new ClaimedReportJob(
                42L,
                ReportType.payments,
                FileType.csv,
                thriftJsonCodec.serialize(reportQuery),
                request.getTimezone(),
                1
        );
    }
}
