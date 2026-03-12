package dev.vality.ccreporter.serialization;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public abstract class ThriftBinaryDeserializer<T extends TBase<?, ?>> implements BinaryDeserializer<T> {

    @Override
    public T deserialize(byte[] bytes) {
        try {
            T value = newInstance();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(value, bytes);
            return value;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to deserialize thrift payload", ex);
        }
    }

    protected abstract T newInstance();
}
