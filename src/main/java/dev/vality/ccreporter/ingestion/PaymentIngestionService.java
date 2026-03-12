package dev.vality.ccreporter.ingestion;

import dev.vality.ccreporter.dao.PaymentCurrentDao;
import dev.vality.ccreporter.serialization.MachineEventPayloadParser;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.machinegun.eventsink.MachineEvent;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentIngestionService {

    private final PaymentCurrentDao paymentCurrentDao;
    private final PaymentEventProjector paymentEventProjector;
    private final MachineEventPayloadParser<EventPayload> paymentEventPayloadMachineEventParser;

    public PaymentIngestionService(
            PaymentCurrentDao paymentCurrentDao,
            PaymentEventProjector paymentEventProjector,
            MachineEventPayloadParser<EventPayload> paymentEventPayloadMachineEventParser
    ) {
        this.paymentCurrentDao = paymentCurrentDao;
        this.paymentEventProjector = paymentEventProjector;
        this.paymentEventPayloadMachineEventParser = paymentEventPayloadMachineEventParser;
    }

    @Transactional
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.stream()
                .sorted((left, right) -> Long.compare(left.getEventId(), right.getEventId()))
                .forEach(this::handleEvent);
    }

    private void handleEvent(MachineEvent event) {
        EventPayload payload = paymentEventPayloadMachineEventParser.parse(event);
        paymentEventProjector.project(event, payload).forEach(paymentCurrentDao::upsert);
    }
}
