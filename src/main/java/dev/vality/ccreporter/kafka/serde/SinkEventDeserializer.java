package dev.vality.ccreporter.kafka.serde;

import dev.vality.kafka.common.serialization.AbstractThriftDeserializer;
import dev.vality.machinegun.eventsink.SinkEvent;

public class SinkEventDeserializer extends AbstractThriftDeserializer<SinkEvent> {

    @Override
    public SinkEvent deserialize(String topic, byte[] data) {
        return deserialize(data, new SinkEvent());
    }
}
