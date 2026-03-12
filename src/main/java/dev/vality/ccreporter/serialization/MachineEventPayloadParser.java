package dev.vality.ccreporter.serialization;

import dev.vality.machinegun.eventsink.MachineEvent;

public class MachineEventPayloadParser<T> {

    private final BinaryDeserializer<T> binaryDeserializer;

    public MachineEventPayloadParser(BinaryDeserializer<T> binaryDeserializer) {
        this.binaryDeserializer = binaryDeserializer;
    }

    public T parse(MachineEvent event) {
        if (event == null || event.getData() == null || !event.getData().isSetBin()) {
            throw new IllegalArgumentException("MachineEvent does not contain binary payload");
        }
        return binaryDeserializer.deserialize(event.getData().getBin());
    }
}
