package dev.vality.ccreporter.ingestion.withdrawal;

import dev.vality.ccreporter.dao.WithdrawalTxnCurrentDao;
import dev.vality.ccreporter.serde.thrift.MachineEventParser;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class WithdrawalIngestionService {

    private final WithdrawalTxnCurrentDao withdrawalTxnCurrentDao;
    private final WithdrawalEventProjector withdrawalEventProjector;
    private final MachineEventParser<TimestampedChange> withdrawalEventMachineEventParser;

    @Transactional
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.stream()
                .sorted(Comparator.comparingLong(MachineEvent::getEventId))
                .forEach(this::handleEvent);
    }

    private void handleEvent(MachineEvent event) {
        var payload = withdrawalEventMachineEventParser.parse(event);
        withdrawalEventProjector.project(event, payload).forEach(withdrawalTxnCurrentDao::upsert);
    }
}
