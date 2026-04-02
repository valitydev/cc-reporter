package dev.vality.ccreporter.kafka.listener;

import dev.vality.ccreporter.ingestion.payment.PaymentIngestionService;
import dev.vality.ccreporter.kafka.support.BatchLoggingKafkaListener;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "ccr.kafka.topics.payments", name = "enabled", havingValue = "true")
public class PaymentEventListener implements BatchLoggingKafkaListener {

    private final PaymentIngestionService paymentIngestionService;

    @KafkaListener(
            topics = "${ccr.kafka.topics.payments.id}",
            containerFactory = "paymentsKafkaListenerContainerFactory"
    )
    public void listen(List<ConsumerRecord<String, SinkEvent>> batch, Acknowledgment acknowledgment) {
        handleBatch(
                "payments",
                batch,
                acknowledgment,
                records -> paymentIngestionService.handleEvents(records.stream().map(ConsumerRecord::value)
                        .map(SinkEvent::getEvent)
                        .toList())
        );
    }
}
