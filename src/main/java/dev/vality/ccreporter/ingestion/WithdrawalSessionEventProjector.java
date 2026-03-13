package dev.vality.ccreporter.ingestion;

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
                var binding = new WithdrawalSessionBindingCurrent();
                binding.setSessionId(event.getSourceId());
                binding.setWithdrawalId(change.getCreated().getWithdrawal().getId());
                binding.setDomainEventId(event.getEventId());
                binding.setDomainEventCreatedAt(toLocalDateTime(eventCreatedAt));
                bindings.add(binding);
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
                var update = new WithdrawalTxnCurrent();
                update.setWithdrawalId(withdrawalId);
                update.setDomainEventId(event.getEventId());
                update.setDomainEventCreatedAt(toLocalDateTime(eventCreatedAt));
                update.setTrxId(trxInfo.getId());
                return Optional.of(update);
            }
        }
        return Optional.empty();
    }

    private LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }
}
