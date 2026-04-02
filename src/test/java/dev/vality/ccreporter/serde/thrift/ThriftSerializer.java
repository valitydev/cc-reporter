package dev.vality.ccreporter.serde.thrift;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.Map;

@Slf4j
public class ThriftSerializer<T extends TBase<?, ?>> implements Serializer<T> {

    private final TProtocolFactory protocolFactory;
    private final ThreadLocal<TSerializer> thriftSerializer = ThreadLocal.withInitial(this::createSerializer);

    public ThriftSerializer() {
        this(new TBinaryProtocol.Factory());
    }

    public ThriftSerializer(TProtocolFactory protocolFactory) {
        this.protocolFactory = protocolFactory;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.debug("ThriftSerializer configure: isKey={}", isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return thriftSerializer.get().serialize(data);
        } catch (TException ex) {
            log.error("Error when serializing thrift data for topic: {}", topic, ex);
            throw new RuntimeException(String.format("Failed to serialize thrift data for topic: %s", topic), ex);
        }
    }

    public byte[] serialize(T data) {
        return serialize("unknown", data);
    }

    @Override
    public void close() {
        thriftSerializer.remove();
    }

    private TSerializer createSerializer() {
        try {
            return new TSerializer(protocolFactory);
        } catch (TException ex) {
            throw new RuntimeException("Failed to initialize Apache Thrift TSerializer", ex);
        }
    }
}
