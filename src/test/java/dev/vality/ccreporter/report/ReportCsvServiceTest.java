package dev.vality.ccreporter.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.dao.ClaimedReportJob;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.util.ThriftQueryCodec;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReportCsvServiceTest {

    @Test
    void paymentsCsvQueryUsesForwardOnlyCursorFetchSize() throws Exception {
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = mock(NamedParameterJdbcTemplate.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        TransactionStatus transactionStatus = new SimpleTransactionStatus();
        ObjectMapper objectMapper = new ObjectMapper();
        ThriftQueryCodec thriftQueryCodec = new ThriftQueryCodec(objectMapper);
        ReportCsvService reportCsvService =
                new ReportCsvService(namedParameterJdbcTemplate, thriftQueryCodec, transactionManager);

        when(namedParameterJdbcTemplate.getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(transactionManager.getTransaction(any(TransactionDefinition.class))).thenReturn(transactionStatus);
        doNothing().when(transactionManager).commit(transactionStatus);
        when(jdbcTemplate.queryForObject("SELECT EXTRACT(EPOCH FROM now())", BigDecimal.class))
                .thenReturn(new BigDecimal("1700000000.123456789"));

        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(
                any(String.class),
                eq(ResultSet.TYPE_FORWARD_ONLY),
                eq(ResultSet.CONCUR_READ_ONLY)
        ))
                .thenReturn(preparedStatement);

        doAnswer(invocation -> {
            PreparedStatementCreator preparedStatementCreator = invocation.getArgument(0);
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

            RowCallbackHandler rowCallbackHandler = invocation.getArgument(1);
            rowCallbackHandler.processRow(resultSet);
            return null;
        }).when(jdbcTemplate).query(any(PreparedStatementCreator.class), any(RowCallbackHandler.class));

        GeneratedCsvReport generatedCsvReport = reportCsvService.generate(claimedPaymentsJob(thriftQueryCodec));

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

    private ClaimedReportJob claimedPaymentsJob(ThriftQueryCodec thriftQueryCodec) {
        CreateReportRequest request = ReportRequestFixtures.payments(
                "cursor-fetch-size-1",
                new TimeRange("2025-12-31T00:00:00Z", "2026-01-02T00:00:00Z")
        );
        request.setTimezone("Asia/Krasnoyarsk");
        ReportQuery reportQuery = request.getQuery();
        return new ClaimedReportJob(
                42L,
                ReportType.payments,
                FileType.csv,
                thriftQueryCodec.serialize(reportQuery),
                request.getTimezone(),
                1
        );
    }
}
