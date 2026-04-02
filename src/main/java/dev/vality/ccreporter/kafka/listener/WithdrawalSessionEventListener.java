package dev.vality.ccreporter.kafka.listener;

import dev.vality.ccreporter.ingestion.withdrawal.session.WithdrawalSessionIngestionService;
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
@ConditionalOnProperty(prefix = "ccr.kafka.topics.withdrawal-sessions", name = "enabled", havingValue = "true")
public class WithdrawalSessionEventListener implements BatchLoggingKafkaListener {

    private final WithdrawalSessionIngestionService withdrawalSessionIngestionService;

    @KafkaListener(
            topics = "${ccr.kafka.topics.withdrawal-sessions.id}",
            containerFactory = "withdrawalSessionsKafkaListenerContainerFactory"
    )
    public void listen(List<ConsumerRecord<String, SinkEvent>> batch, Acknowledgment acknowledgment) {
        handleBatch(
                "withdrawal sessions",
                batch,
                acknowledgment,
                records -> withdrawalSessionIngestionService.handleEvents(records.stream().map(ConsumerRecord::value)
                        .map(SinkEvent::getEvent)
                        .toList())
        );
    }
}
