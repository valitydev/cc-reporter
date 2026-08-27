package dev.vality.ccreporter.serde.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.geck.serializer.kit.json.JsonHandler;
import dev.vality.geck.serializer.kit.json.JsonProcessor;
import dev.vality.geck.serializer.kit.tbase.TBaseHandler;
import dev.vality.geck.serializer.kit.tbase.TBaseProcessor;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TBase;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class ThriftJsonCodec {

    private final ObjectMapper objectMapper;

    public String serialize(TBase value) {
        try {
            return new TBaseProcessor().process(value, new JsonHandler()).toString();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Failed to serialize thrift object to JSON", ex);
        }
    }

    public <T extends TBase> T deserialize(String json, Class<T> thriftClass) {
        try {
            return new JsonProcessor().process(
                    objectMapper.readTree(json),
                    new TBaseHandler<>(thriftClass)
            );
        } catch (IOException ex) {
            throw new IllegalArgumentException("Failed to deserialize thrift object from JSON", ex);
        }
    }
}
