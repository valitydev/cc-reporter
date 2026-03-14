package dev.vality.ccreporter.ingestion.withdrawal.session;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalSessionBindingCurrent;
import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;
import dev.vality.fistful.withdrawal_session.Change;
import dev.vality.fistful.withdrawal_session.Event;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static dev.vality.ccreporter.util.TimestampUtils.toOptionalLocalDateTime;

@Component
public class WithdrawalSessionEventProjector {

    public List<WithdrawalSessionBindingCurrent> projectBindings(MachineEvent event, Event payload) {
        var bindings = new ArrayList<WithdrawalSessionBindingCurrent>();
        if (payload == null || payload.getChanges() == null) {
            return bindings;
        }
        var eventCreatedAt = Instant.parse(event.getCreatedAt());
        for (Change change : payload.getChanges()) {
            if (change.isSetCreated()) {
                bindings.add(bindingUpdate(
                        event.getSourceId(),
                        change.getCreated().getWithdrawal().getId(),
                        event.getEventId(),
                        eventCreatedAt
                ));
            }
        }
        return bindings;
    }

    public Optional<WithdrawalTxnCurrent> projectTransactionBound(
            MachineEvent event,
            Event payload,
            String withdrawalId
    ) {
        if (payload == null || payload.getChanges() == null || withdrawalId == null) {
            return Optional.empty();
        }
        var eventCreatedAt = Instant.parse(event.getCreatedAt());
        for (Change change : payload.getChanges()) {
            if (change.isSetTransactionBound()) {
                var trxInfo = change.getTransactionBound().getTrxInfo();
                return Optional.of(new WithdrawalTxnCurrent()
                        .setWithdrawalId(withdrawalId)
                        .setDomainEventId(event.getEventId())
                        .setDomainEventCreatedAt(toOptionalLocalDateTime(eventCreatedAt))
                        .setTrxId(trxInfo.getId()));
            }
        }
        return Optional.empty();
    }

    private WithdrawalSessionBindingCurrent bindingUpdate(
            String sessionId,
            String withdrawalId,
            long domainEventId,
            Instant eventCreatedAt
    ) {
        return new WithdrawalSessionBindingCurrent()
                .setSessionId(sessionId)
                .setWithdrawalId(withdrawalId)
                .setDomainEventId(domainEventId)
                .setDomainEventCreatedAt(toOptionalLocalDateTime(eventCreatedAt));
    }
}
