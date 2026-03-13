package dev.vality.ccreporter.integration.fixture;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.domain.tables.pojos.WithdrawalSessionBindingCurrent;
import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Собирает доменные апдейты для DAO-тестов, чтобы сценарии не расползались из-за ручной сборки всех полей.
 */
public final class CurrentStateUpdateFixtures {

    private CurrentStateUpdateFixtures() {
    }

    public static PaymentTxnCurrent paymentUpdate(long eventId, String status, Instant finalizedAt) {
        var update = new PaymentTxnCurrent();
        update.setInvoiceId("invoice-1");
        update.setPaymentId("payment-1");
        update.setDomainEventId(eventId);
        update.setDomainEventCreatedAt(TimestampUtils.toLocalDateTime(
                Instant.parse("2026-01-01T10:00:00Z").plusSeconds(eventId)
        ));
        update.setPartyId("party-1");
        update.setShopId("shop-1");
        update.setCreatedAt(toLocalDateTime(Instant.parse("2026-01-01T10:00:00Z")));
        update.setFinalizedAt(toLocalDateTime(finalizedAt));
        update.setStatus(status);
        update.setProviderId("provider-1");
        update.setTerminalId("terminal-1");
        update.setAmount(1000L);
        update.setFee(10L);
        update.setCurrency("RUB");
        update.setTrxId("trx-1");
        update.setExternalId("external-1");
        update.setRrn("rrn-1");
        update.setApprovalCode("approval-1");
        update.setPaymentToolType("bank_card");
        update.setOriginalAmount(1000L);
        update.setOriginalCurrency("RUB");
        update.setConvertedAmount(1000L);
        update.setExchangeRateInternal(BigDecimal.ONE);
        update.setProviderAmount(1000L);
        update.setProviderCurrency("RUB");
        return update;
    }

    public static WithdrawalTxnCurrent withdrawalUpdate(
            long eventId,
            String status,
            Instant finalizedAt,
            String trxId
    ) {
        var update = new WithdrawalTxnCurrent();
        update.setWithdrawalId("withdrawal-1");
        update.setDomainEventId(eventId);
        update.setDomainEventCreatedAt(TimestampUtils.toLocalDateTime(
                Instant.parse("2026-01-01T11:00:00Z").plusSeconds(eventId)
        ));
        update.setPartyId("party-1");
        update.setWalletId("wallet-1");
        update.setDestinationId("destination-1");
        update.setCreatedAt(toLocalDateTime(Instant.parse("2026-01-01T11:00:00Z")));
        update.setFinalizedAt(toLocalDateTime(finalizedAt));
        update.setStatus(status);
        update.setProviderId("provider-1");
        update.setTerminalId("terminal-1");
        update.setAmount(2000L);
        update.setFee(20L);
        update.setCurrency("RUB");
        update.setTrxId(trxId);
        update.setExternalId("external-1");
        update.setErrorCode("failure");
        update.setErrorReason("reason");
        update.setErrorSubFailure("sub");
        update.setOriginalAmount(2100L);
        update.setOriginalCurrency("USD");
        update.setConvertedAmount(2000L);
        update.setExchangeRateInternal(new BigDecimal("0.9523809524"));
        update.setProviderAmount(2000L);
        update.setProviderCurrency("RUB");
        return update;
    }

    public static WithdrawalSessionBindingCurrent withdrawalSessionBindingUpdate(
            String sessionId,
            String withdrawalId,
            long eventId,
            Instant eventCreatedAt
    ) {
        var update = new WithdrawalSessionBindingCurrent();
        update.setSessionId(sessionId);
        update.setWithdrawalId(withdrawalId);
        update.setDomainEventId(eventId);
        update.setDomainEventCreatedAt(toLocalDateTime(eventCreatedAt));
        return update;
    }

    private static LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }
}
