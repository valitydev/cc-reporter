package dev.vality.ccreporter.kafka.serde;

import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.kafka.common.serialization.AbstractThriftDeserializer;

public class HistoricalCommitDeserializer extends AbstractThriftDeserializer<HistoricalCommit> {

    @Override
    public HistoricalCommit deserialize(String topic, byte[] data) {
        return deserialize(data, new HistoricalCommit());
    }
}
