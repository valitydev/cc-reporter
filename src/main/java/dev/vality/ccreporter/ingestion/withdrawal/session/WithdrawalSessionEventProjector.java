package dev.vality.ccreporter.ingestion.withdrawal.session;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalSession;
import dev.vality.fistful.withdrawal_session.Change;
import dev.vality.fistful.withdrawal_session.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static dev.vality.ccreporter.util.SearchValueNormalizer.normalize;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;

@Component
public class WithdrawalSessionEventProjector {

    public List<WithdrawalSession> project(MachineEvent event, TimestampedChange payload) {
        var updates = new ArrayList<WithdrawalSession>();
        if (payload == null || payload.getChange() == null) {
            return updates;
        }
        projectChange(event, payload.getChange()).ifPresent(updates::add);
        return updates;
    }

    private Optional<WithdrawalSession> projectChange(MachineEvent event, Change change) {
        return createdUpdate(event, change)
                .or(() -> transactionBoundUpdate(event, change));
    }

    private Optional<WithdrawalSession> createdUpdate(MachineEvent event, Change change) {
        if (!change.isSetCreated()) {
            return Optional.empty();
        }
        return Optional.of(baseUpdate(event)
                .setWithdrawalId(change.getCreated().getWithdrawal().getId()));
    }

    private Optional<WithdrawalSession> transactionBoundUpdate(MachineEvent event, Change change) {
        if (!change.isSetTransactionBound()) {
            return Optional.empty();
        }
        var trxInfo = change.getTransactionBound().getTrxInfo();
        return Optional.of(baseUpdate(event)
                .setTrxId(trxInfo.getId())
                .setTrxSearch(normalize(trxInfo.getId())));
    }

    private WithdrawalSession baseUpdate(MachineEvent event) {
        return new WithdrawalSession()
                .setSessionId(event.getSourceId())
                .setDomainEventId(event.getEventId())
                .setDomainEventCreatedAt(toLocalDateTime(event.getCreatedAt()));
    }
}
