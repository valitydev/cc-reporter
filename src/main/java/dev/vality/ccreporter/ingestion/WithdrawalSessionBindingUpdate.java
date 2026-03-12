package dev.vality.ccreporter.ingestion;

import java.time.Instant;

public record WithdrawalSessionBindingUpdate(
        String sessionId,
        String withdrawalId,
        long domainEventId,
        Instant domainEventCreatedAt
) {
}
