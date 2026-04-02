package dev.vality.ccreporter.kafka.listener;

import dev.vality.ccreporter.ingestion.withdrawal.WithdrawalIngestionService;
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
@ConditionalOnProperty(prefix = "ccr.kafka.topics.withdrawals", name = "enabled", havingValue = "true")
public class WithdrawalEventListener implements BatchLoggingKafkaListener {

    private final WithdrawalIngestionService withdrawalIngestionService;

    @KafkaListener(
            topics = "${ccr.kafka.topics.withdrawals.id}",
            containerFactory = "withdrawalsKafkaListenerContainerFactory"
    )
    public void listen(List<ConsumerRecord<String, SinkEvent>> batch, Acknowledgment acknowledgment) {
        handleBatch(
                "withdrawals",
                batch,
                acknowledgment,
                records -> withdrawalIngestionService.handleEvents(records.stream().map(ConsumerRecord::value)
                        .map(SinkEvent::getEvent)
                        .toList())
        );
    }
}
