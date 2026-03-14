package dev.vality.ccreporter.kafka;

import dev.vality.ccreporter.ingestion.withdrawal.session.WithdrawalSessionIngestionService;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "ccr.kafka.topics.withdrawal-sessions", name = "enabled", havingValue = "true")
public class WithdrawalSessionEventListener {

    private final WithdrawalSessionIngestionService withdrawalSessionIngestionService;

    @KafkaListener(
            topics = "${ccr.kafka.topics.withdrawal-sessions.id}",
            containerFactory = "withdrawalSessionsKafkaListenerContainerFactory"
    )
    public void listen(List<SinkEvent> batch, Acknowledgment acknowledgment) {
        withdrawalSessionIngestionService.handleEvents(batch.stream().map(SinkEvent::getEvent).toList());
        acknowledgment.acknowledge();
    }
}
