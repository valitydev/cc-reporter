package dev.vality.ccreporter.serialization;

import dev.vality.machinegun.eventsink.MachineEvent;
import org.apache.thrift.TBase;

public record MachineEventPayloadParser<T extends TBase<?, ?>>(ThriftBinaryDeserializer<T> binaryDeserializer) {

    public T parse(MachineEvent event) {
        if (event == null || event.getData() == null || !event.getData().isSetBin()) {
            throw new IllegalArgumentException("MachineEvent does not contain binary payload");
        }
        return binaryDeserializer.deserialize(event.getData().getBin());
    }
}
