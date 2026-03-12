package dev.vality.ccreporter.kafka;

import dev.vality.ccreporter.ingestion.WithdrawalIngestionService;
import dev.vality.machinegun.eventsink.SinkEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnProperty(prefix = "ccr.kafka.topics.withdrawals", name = "enabled", havingValue = "true")
public class WithdrawalEventListener {

    private final WithdrawalIngestionService withdrawalIngestionService;

    public WithdrawalEventListener(WithdrawalIngestionService withdrawalIngestionService) {
        this.withdrawalIngestionService = withdrawalIngestionService;
    }

    @KafkaListener(
            topics = "${ccr.kafka.topics.withdrawals.id}",
            containerFactory = "withdrawalsKafkaListenerContainerFactory"
    )
    public void listen(List<SinkEvent> batch, Acknowledgment acknowledgment) {
        withdrawalIngestionService.handleEvents(batch.stream().map(SinkEvent::getEvent).toList());
        acknowledgment.acknowledge();
    }
}
