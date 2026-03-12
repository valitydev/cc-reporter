package dev.vality.ccreporter.ingestion;

import dev.vality.ccreporter.dao.WithdrawalCurrentDao;
import dev.vality.ccreporter.serialization.MachineEventPayloadParser;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class WithdrawalIngestionService {

    private final WithdrawalCurrentDao withdrawalCurrentDao;
    private final WithdrawalEventProjector withdrawalEventProjector;
    private final MachineEventPayloadParser<Event> withdrawalEventMachineEventParser;

    public WithdrawalIngestionService(
            WithdrawalCurrentDao withdrawalCurrentDao,
            WithdrawalEventProjector withdrawalEventProjector,
            MachineEventPayloadParser<Event> withdrawalEventMachineEventParser
    ) {
        this.withdrawalCurrentDao = withdrawalCurrentDao;
        this.withdrawalEventProjector = withdrawalEventProjector;
        this.withdrawalEventMachineEventParser = withdrawalEventMachineEventParser;
    }

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
