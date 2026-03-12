package dev.vality.ccreporter.serialization;

import dev.vality.machinegun.eventsink.MachineEvent;
import org.apache.thrift.TBase;

public class MachineEventPayloadParser<T extends TBase<?, ?>> {

    private final ThriftBinaryDeserializer<T> binaryDeserializer;

    public MachineEventPayloadParser(ThriftBinaryDeserializer<T> binaryDeserializer) {
        this.binaryDeserializer = binaryDeserializer;
    }

    public T parse(MachineEvent event) {
        if (event == null || event.getData() == null || !event.getData().isSetBin()) {
            throw new IllegalArgumentException("MachineEvent does not contain binary payload");
        }
        return binaryDeserializer.deserialize(event.getData().getBin());
    }
}
