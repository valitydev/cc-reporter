package dev.vality.ccreporter.ingestion.withdrawal.session;

import dev.vality.ccreporter.dao.WithdrawalCurrentDao;
import dev.vality.ccreporter.dao.WithdrawalSessionBindingDao;
import dev.vality.ccreporter.serde.thrift.MachineEventParser;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class WithdrawalSessionIngestionService {

    private final WithdrawalSessionBindingDao withdrawalSessionBindingDao;
    private final WithdrawalCurrentDao withdrawalCurrentDao;
    private final WithdrawalSessionEventProjector withdrawalSessionEventProjector;
    private final MachineEventParser<dev.vality.fistful.withdrawal_session.Event>
            withdrawalSessionEventMachineEventParser;

    @Transactional
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.stream()
                .sorted((left, right) -> Long.compare(left.getEventId(), right.getEventId()))
                .forEach(this::handleEvent);
    }

    private void handleEvent(MachineEvent event) {
        var payload = withdrawalSessionEventMachineEventParser.parse(event);
        withdrawalSessionEventProjector.projectBindings(event, payload).forEach(withdrawalSessionBindingDao::upsert);
        withdrawalSessionBindingDao.findWithdrawalId(event.getSourceId())
                .flatMap(withdrawalId -> withdrawalSessionEventProjector.projectTransactionBound(event, payload,
                        withdrawalId))
                .ifPresent(withdrawalCurrentDao::upsert);
    }
}
