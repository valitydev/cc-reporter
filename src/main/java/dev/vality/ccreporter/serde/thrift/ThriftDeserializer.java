package dev.vality.ccreporter.serde.thrift;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class ThriftDeserializer<T extends TBase<?, ?>> implements Deserializer<T> {

    private final Supplier<T> factory;
    private final TProtocolFactory protocolFactory;
    private final ThreadLocal<TDeserializer> thriftDeserializer = ThreadLocal.withInitial(this::createDeserializer);

    public ThriftDeserializer(Supplier<T> factory) {
        this(factory, new TBinaryProtocol.Factory());
    }

    public ThriftDeserializer(Supplier<T> factory, TProtocolFactory protocolFactory) {
        this.factory = factory;
        this.protocolFactory = protocolFactory;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.debug("ThriftDeserializer configure: isKey={}", isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        var instance = factory.get();
        try {
            thriftDeserializer.get().deserialize(instance, data);
            return instance;
        } catch (TException ex) {
            log.error("Error when deserializing thrift data from topic: {}", topic, ex);
            throw new RuntimeException(String.format("Failed to deserialize thrift data from topic: %s", topic), ex);
        }
    }

    public T deserialize(byte[] data) {
        return deserialize("unknown", data);
    }

    @Override
    public void close() {
        thriftDeserializer.remove();
    }

    private TDeserializer createDeserializer() {
        try {
            return new TDeserializer(protocolFactory);
        } catch (TException ex) {
            throw new RuntimeException("Failed to initialize Apache Thrift TDeserializer", ex);
        }
    }
}
