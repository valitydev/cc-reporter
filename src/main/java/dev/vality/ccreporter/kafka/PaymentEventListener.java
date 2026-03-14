package dev.vality.ccreporter.kafka;

import dev.vality.ccreporter.ingestion.payment.PaymentIngestionService;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "ccr.kafka.topics.payments", name = "enabled", havingValue = "true")
public class PaymentEventListener {

    private final PaymentIngestionService paymentIngestionService;

    @KafkaListener(
            topics = "${ccr.kafka.topics.payments.id}",
            containerFactory = "paymentsKafkaListenerContainerFactory"
    )
    public void listen(List<SinkEvent> batch, Acknowledgment acknowledgment) {
        paymentIngestionService.handleEvents(batch.stream().map(SinkEvent::getEvent).toList());
        acknowledgment.acknowledge();
    }
}
