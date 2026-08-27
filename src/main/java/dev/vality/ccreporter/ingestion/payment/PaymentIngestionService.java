package dev.vality.ccreporter.ingestion.payment;

import dev.vality.ccreporter.dao.PaymentTxnCurrentDao;
import dev.vality.ccreporter.serde.thrift.MachineEventParser;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class PaymentIngestionService {

    private final PaymentTxnCurrentDao paymentTxnCurrentDao;
    private final PaymentEventProjector paymentEventProjector;
    private final MachineEventParser<EventPayload> paymentEventPayloadMachineEventParser;

    @Transactional
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.stream()
                .sorted(Comparator.comparingLong(MachineEvent::getEventId))
                .forEach(this::handleEvent);
    }

    private void handleEvent(MachineEvent event) {
        var payload = paymentEventPayloadMachineEventParser.parse(event);
        paymentEventProjector.project(event, payload).forEach(paymentTxnCurrentDao::upsert);
    }
}
