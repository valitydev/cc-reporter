package dev.vality.ccreporter.kafka.support;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.function.Consumer;

public interface BatchLoggingKafkaListener {

    default <K, V> void handleBatch(
            String batchType,
            List<ConsumerRecord<K, V>> batch,
            Acknowledgment acknowledgment,
            Consumer<List<ConsumerRecord<K, V>>> handler
    ) {
        handler.accept(batch);
        LoggerFactory.getLogger(getClass()).info(
                "Processed {} batch: {}",
                batchType,
                BatchConsumerLogUtil.toSummaryString(batch)
        );
        acknowledgment.acknowledge();
    }
}
