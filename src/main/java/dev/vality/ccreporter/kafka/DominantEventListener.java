package dev.vality.ccreporter.kafka;

import dev.vality.ccreporter.ingestion.dominant.DominantLookupIngestionService;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "ccr.kafka.topics.dominant", name = "enabled", havingValue = "true")
public class DominantEventListener {

    private final DominantLookupIngestionService dominantLookupIngestionService;

    @KafkaListener(
            topics = "${ccr.kafka.topics.dominant.id}",
            containerFactory = "dominantKafkaListenerContainerFactory"
    )
    public void listen(List<HistoricalCommit> batch, Acknowledgment acknowledgment) {
        dominantLookupIngestionService.handleCommits(batch);
        acknowledgment.acknowledge();
    }
}
