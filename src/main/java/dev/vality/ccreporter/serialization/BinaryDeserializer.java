package dev.vality.ccreporter.serialization;

public interface BinaryDeserializer<T> {

    T deserialize(byte[] bytes);
}
