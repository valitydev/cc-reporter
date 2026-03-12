package dev.vality.ccreporter.integration.fixture;

import dev.vality.ccreporter.ingestion.PaymentCurrentUpdate;
import dev.vality.ccreporter.ingestion.WithdrawalCurrentUpdate;
import dev.vality.ccreporter.ingestion.WithdrawalSessionBindingUpdate;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Собирает доменные апдейты для DAO-тестов, чтобы сценарии не расползались из-за ручной сборки всех полей.
 */
public final class CurrentStateUpdateFixtures {

    private CurrentStateUpdateFixtures() {
    }

    public static PaymentCurrentUpdate paymentUpdate(long eventId, String status, Instant finalizedAt) {
        return new PaymentCurrentUpdate(
                "invoice-1",
                "payment-1",
                eventId,
                Instant.parse("2026-01-01T10:00:00Z").plusSeconds(eventId),
                "party-1",
                "shop-1",
                "Shop One",
                Instant.parse("2026-01-01T10:00:00Z"),
                finalizedAt,
                status,
                "provider-1",
                null,
                "terminal-1",
                null,
                1000L,
                10L,
                "RUB",
                "trx-1",
                "external-1",
                "rrn-1",
                "approval-1",
                "bank_card",
                1000L,
                "RUB",
                1000L,
                BigDecimal.ONE,
                1000L,
                "RUB"
        );
    }

    public static WithdrawalCurrentUpdate withdrawalUpdate(
            long eventId,
            String status,
            Instant finalizedAt,
            String trxId
    ) {
        return new WithdrawalCurrentUpdate(
                "withdrawal-1",
                eventId,
                Instant.parse("2026-01-01T11:00:00Z").plusSeconds(eventId),
                "party-1",
                "wallet-1",
                "Wallet One",
                "destination-1",
                Instant.parse("2026-01-01T11:00:00Z"),
                finalizedAt,
                status,
                "provider-1",
                null,
                "terminal-1",
                null,
                2000L,
                20L,
                "RUB",
                trxId,
                "external-1",
                "failure",
                "reason",
                "sub",
                2100L,
                "USD",
                2000L,
                new BigDecimal("0.9523809524"),
                2000L,
                "RUB"
        );
    }

    public static WithdrawalSessionBindingUpdate withdrawalSessionBindingUpdate(
            String sessionId,
            String withdrawalId,
            long eventId,
            Instant eventCreatedAt
    ) {
        return new WithdrawalSessionBindingUpdate(sessionId, withdrawalId, eventId, eventCreatedAt);
    }
}
