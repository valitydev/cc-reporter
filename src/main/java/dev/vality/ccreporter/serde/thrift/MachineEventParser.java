package dev.vality.ccreporter.serde.thrift;

import dev.vality.machinegun.eventsink.MachineEvent;
import org.apache.thrift.TBase;

public record MachineEventParser<T extends TBase<?, ?>>(ThriftDeserializer<T> deserializer) {

    public T parse(MachineEvent event) {
        if (event == null || event.getData() == null || !event.getData().isSetBin()) {
            throw new IllegalArgumentException("MachineEvent does not contain binary payload");
        }
        return deserializer.deserialize(event.getData().getBin());
    }
}
