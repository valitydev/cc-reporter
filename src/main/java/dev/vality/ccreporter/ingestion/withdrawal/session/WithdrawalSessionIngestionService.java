package dev.vality.ccreporter.ingestion.withdrawal.session;

import dev.vality.ccreporter.dao.WithdrawalSessionDao;
import dev.vality.ccreporter.serde.thrift.MachineEventParser;
import dev.vality.fistful.withdrawal_session.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class WithdrawalSessionIngestionService {

    private final WithdrawalSessionDao withdrawalSessionDao;
    private final WithdrawalSessionEventProjector withdrawalSessionEventProjector;
    private final MachineEventParser<TimestampedChange> withdrawalSessionEventMachineEventParser;

    @Transactional
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.stream()
                .sorted(Comparator.comparingLong(MachineEvent::getEventId))
                .forEach(this::handleEvent);
    }

    private void handleEvent(MachineEvent event) {
        var payload = withdrawalSessionEventMachineEventParser.parse(event);
        withdrawalSessionEventProjector.project(event, payload).forEach(withdrawalSessionDao::upsert);
    }
}
