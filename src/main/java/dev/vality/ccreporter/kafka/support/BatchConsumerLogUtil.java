package dev.vality.ccreporter.kafka.support;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public final class BatchConsumerLogUtil {

    public static <K, V> String toSummaryString(List<ConsumerRecord<K, V>> records) {
        if (records.isEmpty()) {
            return "empty";
        }

        return records.stream()
                .collect(Collectors.groupingBy(ConsumerRecord::partition))
                .values()
                .stream()
                .map(BatchConsumerLogUtil::toPartitionSummaryString)
                .collect(Collectors.joining("; "));
    }

    private static <K, V> String toPartitionSummaryString(List<ConsumerRecord<K, V>> records) {
        var firstRecord = records.getFirst();
        var lastRecord = records.getLast();
        var keySizeSummary = records.stream().mapToLong(ConsumerRecord::serializedKeySize).summaryStatistics();
        var valueSizeSummary = records.stream().mapToLong(ConsumerRecord::serializedValueSize).summaryStatistics();
        return String.format(
                "topic='%s', partition=%d, offset={%d...%d}, createdAt={%s...%s}, " +
                        "keySize={min=%d, max=%d, avg=%.2f}, valueSize={min=%d, max=%d, avg=%.2f}, count=%d",
                firstRecord.topic(),
                firstRecord.partition(),
                firstRecord.offset(),
                lastRecord.offset(),
                Instant.ofEpochMilli(firstRecord.timestamp()),
                Instant.ofEpochMilli(lastRecord.timestamp()),
                keySizeSummary.getMin(),
                keySizeSummary.getMax(),
                keySizeSummary.getAverage(),
                valueSizeSummary.getMin(),
                valueSizeSummary.getMax(),
                valueSizeSummary.getAverage(),
                records.size()
        );
    }
}
