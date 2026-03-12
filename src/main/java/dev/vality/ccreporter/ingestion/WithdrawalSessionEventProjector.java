package dev.vality.ccreporter.ingestion;

import dev.vality.fistful.base.TransactionInfo;
import dev.vality.fistful.withdrawal_session.Change;
import dev.vality.fistful.withdrawal_session.Event;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class WithdrawalSessionEventProjector {

    public List<WithdrawalSessionBindingUpdate> projectBindings(MachineEvent event, Event payload) {
        List<WithdrawalSessionBindingUpdate> bindings = new ArrayList<>();
        if (payload == null || payload.getChanges() == null) {
            return bindings;
        }
        Instant eventCreatedAt = Instant.parse(event.getCreatedAt());
        for (Change change : payload.getChanges()) {
            if (change.isSetCreated()) {
                bindings.add(new WithdrawalSessionBindingUpdate(
                        event.getSourceId(),
                        change.getCreated().getWithdrawal().getId(),
                        event.getEventId(),
                        eventCreatedAt
                ));
            }
        }
        return bindings;
    }

    public Optional<WithdrawalCurrentUpdate> projectTransactionBound(
            MachineEvent event,
            Event payload,
            String withdrawalId
    ) {
        if (payload == null || payload.getChanges() == null || withdrawalId == null) {
            return Optional.empty();
        }
        Instant eventCreatedAt = Instant.parse(event.getCreatedAt());
        for (Change change : payload.getChanges()) {
            if (change.isSetTransactionBound()) {
                TransactionInfo trxInfo = change.getTransactionBound().getTrxInfo();
                return Optional.of(new WithdrawalCurrentUpdate(
                        withdrawalId,
                        event.getEventId(),
                        eventCreatedAt,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        trxInfo.getId(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                ));
            }
        }
        return Optional.empty();
    }
}
