package dev.vality.ccreporter.ingestion;

import dev.vality.ccreporter.dao.WithdrawalCurrentDao;
import dev.vality.ccreporter.dao.WithdrawalSessionBindingDao;
import dev.vality.ccreporter.serialization.MachineEventPayloadParser;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class WithdrawalSessionIngestionService {

    private final WithdrawalSessionBindingDao withdrawalSessionBindingDao;
    private final WithdrawalCurrentDao withdrawalCurrentDao;
    private final WithdrawalSessionEventProjector withdrawalSessionEventProjector;
    private final MachineEventPayloadParser<dev.vality.fistful.withdrawal_session.Event>
            withdrawalSessionEventMachineEventParser;

    public WithdrawalSessionIngestionService(
            WithdrawalSessionBindingDao withdrawalSessionBindingDao,
            WithdrawalCurrentDao withdrawalCurrentDao,
            WithdrawalSessionEventProjector withdrawalSessionEventProjector,
            MachineEventPayloadParser<dev.vality.fistful.withdrawal_session.Event>
                    withdrawalSessionEventMachineEventParser
    ) {
        this.withdrawalSessionBindingDao = withdrawalSessionBindingDao;
        this.withdrawalCurrentDao = withdrawalCurrentDao;
        this.withdrawalSessionEventProjector = withdrawalSessionEventProjector;
        this.withdrawalSessionEventMachineEventParser = withdrawalSessionEventMachineEventParser;
    }

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
