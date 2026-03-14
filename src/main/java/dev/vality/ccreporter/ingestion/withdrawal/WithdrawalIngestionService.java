package dev.vality.ccreporter.ingestion.withdrawal;

import dev.vality.ccreporter.dao.WithdrawalCurrentDao;
import dev.vality.ccreporter.serde.thrift.MachineEventParser;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class WithdrawalIngestionService {

    private final WithdrawalCurrentDao withdrawalCurrentDao;
    private final WithdrawalEventProjector withdrawalEventProjector;
    private final MachineEventParser<Event> withdrawalEventMachineEventParser;


    @Transactional
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.stream()
                .sorted((left, right) -> Long.compare(left.getEventId(), right.getEventId()))
                .forEach(this::handleEvent);
    }

    private void handleEvent(MachineEvent event) {
        var payload = withdrawalEventMachineEventParser.parse(event);
        withdrawalEventProjector.project(event, payload).forEach(withdrawalCurrentDao::upsert);
    }
}
