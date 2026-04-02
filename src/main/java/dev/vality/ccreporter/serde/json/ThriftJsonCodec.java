package dev.vality.ccreporter.serde.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.CollectionMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.*;

@Component
@RequiredArgsConstructor
public class ThriftJsonCodec {

    private final ObjectMapper objectMapper;

    public <T extends TBase<T, ?>> String serialize(T value) {
        try {
            return objectMapper.writeValueAsString(
                    toJsonNode(value, value.getClass(), null, new IdentityHashMap<>())
            );
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Failed to serialize thrift object to JSON", ex);
        }
    }

    public <T extends TBase<T, ?>> T deserialize(String json, Class<T> thriftClass) {
        try {
            return deserialize(objectMapper.readTree(json), thriftClass);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Failed to deserialize thrift object from JSON", ex);
        }
    }

    public <T extends TBase<T, ?>> T deserialize(JsonNode jsonNode, Class<T> thriftClass) {
        return thriftClass.cast(fromJsonNode(jsonNode, thriftClass, null));
    }

    private JsonNode toJsonNode(
            Object value,
            Type declaredType,
            FieldValueMetaData metaData,
            IdentityHashMap<TBase<?, ?>, Boolean> visitingStructs
    ) {
        return switch (value) {
            case null -> objectMapper.nullNode();
            case TUnion<?, ?> unionValue -> serializeUnion(unionValue, visitingStructs);
            case TBase<?, ?> baseValue -> serializeStruct(baseValue, visitingStructs);
            case TEnum enumValue -> objectMapper.getNodeFactory().textNode(((Enum<?>) enumValue).name());
            case ByteBuffer byteBuffer -> objectMapper.getNodeFactory().binaryNode(copyBinary(byteBuffer));
            case byte[] bytes -> objectMapper.getNodeFactory().binaryNode(bytes);
            case Collection<?> collection -> serializeCollection(collection, declaredType, metaData, visitingStructs);
            case Map<?, ?> map -> serializeMap(map, declaredType, metaData, visitingStructs);
            default -> objectMapper.valueToTree(value);
        };
    }

    private ObjectNode serializeStruct(TBase<?, ?> value, IdentityHashMap<TBase<?, ?>, Boolean> visitingStructs) {
        if (visitingStructs.put(value, Boolean.TRUE) != null) {
            throw new IllegalArgumentException(
                    "Cyclic reference detected while serializing thrift struct: " + value.getClass().getName()
            );
        }
        try {
            var node = objectMapper.createObjectNode();
            for (var entry : getFieldMetaDataMap(value.getClass()).entrySet()) {
                var field = entry.getKey();
                if (!isSet(value, field)) {
                    continue;
                }
                var fieldMetaData = entry.getValue();
                var fieldValue = getFieldValue(value, field);
                var declaredType = resolveFieldType(value.getClass(), fieldMetaData.fieldName);
                node.set(
                        fieldMetaData.fieldName,
                        toJsonNode(fieldValue, declaredType, fieldMetaData.valueMetaData, visitingStructs)
                );
            }
            return node;
        } finally {
            visitingStructs.remove(value);
        }
    }

    private ObjectNode serializeUnion(TUnion<?, ?> value, IdentityHashMap<TBase<?, ?>, Boolean> visitingStructs) {
        var setField = value.getSetField();
        if (setField == null) {
            throw new IllegalArgumentException(
                    "Union must have exactly one active field: " + value.getClass().getName());
        }
        var fieldMetaData = getFieldMetaData(value.getClass(), setField);
        var declaredType = resolveFieldType(value.getClass(), fieldMetaData.fieldName);
        var node = objectMapper.createObjectNode();
        node.set(
                fieldMetaData.fieldName,
                toJsonNode(value.getFieldValue(), declaredType, fieldMetaData.valueMetaData, visitingStructs)
        );
        return node;
    }

    private ArrayNode serializeCollection(
            Collection<?> collection,
            Type declaredType,
            FieldValueMetaData metaData,
            IdentityHashMap<TBase<?, ?>, Boolean> visitingStructs
    ) {
        var node = objectMapper.createArrayNode();
        var elementType = declaredType instanceof ParameterizedType parameterizedType
                ? parameterizedType.getActualTypeArguments()[0]
                : Object.class;
        var elementMetaData = metaData instanceof CollectionMetaData collectionMetaData
                ? collectionMetaData.elemMetaData
                : null;
        for (var element : collection) {
            node.add(toJsonNode(element, elementType, elementMetaData, visitingStructs));
        }
        return node;
    }

    private JsonNode serializeMap(
            Map<?, ?> map,
            Type declaredType,
            FieldValueMetaData metaData,
            IdentityHashMap<TBase<?, ?>, Boolean> visitingStructs
    ) {
        var keyType = declaredType instanceof ParameterizedType parameterizedType
                ? parameterizedType.getActualTypeArguments()[0]
                : Object.class;
        var valueType = declaredType instanceof ParameterizedType parameterizedType
                ? parameterizedType.getActualTypeArguments()[1]
                : Object.class;
        var keyMetaData = metaData instanceof MapMetaData mapMetaData ? mapMetaData.keyMetaData : null;
        var valueMetaData = metaData instanceof MapMetaData mapMetaData ? mapMetaData.valueMetaData : null;
        if (map.keySet().stream().allMatch(this::isObjectKeyCompatible)) {
            var node = objectMapper.createObjectNode();
            for (var entry : map.entrySet()) {
                node.set(
                        stringifyMapKey(entry.getKey()),
                        toJsonNode(entry.getValue(), valueType, valueMetaData, visitingStructs)
                );
            }
            return node;
        }
        var node = objectMapper.createArrayNode();
        for (var entry : map.entrySet()) {
            var itemNode = objectMapper.createObjectNode();
            itemNode.set("key", toJsonNode(entry.getKey(), keyType, keyMetaData, visitingStructs));
            itemNode.set("value", toJsonNode(entry.getValue(), valueType, valueMetaData, visitingStructs));
            node.add(itemNode);
        }
        return node;
    }

    private Object fromJsonNode(JsonNode node, Type declaredType, FieldValueMetaData metaData) {
        if (node == null || node.isNull()) {
            return null;
        }
        var rawClass = rawClassOf(declaredType);
        if (TUnion.class.isAssignableFrom(rawClass)) {
            return deserializeUnion(node, rawClass.asSubclass(TUnion.class));
        }
        if (TBase.class.isAssignableFrom(rawClass)) {
            return deserializeStruct(node, rawClass.asSubclass(TBase.class));
        }
        if (Enum.class.isAssignableFrom(rawClass) && TEnum.class.isAssignableFrom(rawClass)) {
            return deserializeEnum(node, rawClass);
        }
        if (Collection.class.isAssignableFrom(rawClass)) {
            return deserializeCollection(node, declaredType, metaData, rawClass);
        }
        if (Map.class.isAssignableFrom(rawClass)) {
            return deserializeMap(node, declaredType, metaData);
        }
        if (ByteBuffer.class.isAssignableFrom(rawClass)) {
            return ByteBuffer.wrap(readBinary(node));
        }
        if (byte[].class.equals(rawClass)) {
            return readBinary(node);
        }
        if (UUID.class.equals(rawClass)) {
            return UUID.fromString(node.asText());
        }
        return objectMapper.convertValue(node, rawClass);
    }

    private TBase<?, ?> deserializeStruct(JsonNode node, Class<? extends TBase> thriftClass) {
        if (!node.isObject()) {
            throw new IllegalArgumentException("Expected JSON object for thrift struct: " + thriftClass.getName());
        }
        var instance = newInstance(thriftClass);
        for (var entry : getFieldMetaDataMap(thriftClass).entrySet()) {
            var fieldMetaData = entry.getValue();
            var fieldNode = node.get(fieldMetaData.fieldName);
            if (fieldNode == null || fieldNode.isNull()) {
                continue;
            }
            var declaredType = resolveFieldType(thriftClass, fieldMetaData.fieldName);
            var fieldValue = fromJsonNode(fieldNode, declaredType, fieldMetaData.valueMetaData);
            instance.setFieldValue(entry.getKey(), fieldValue);
        }
        return instance;
    }

    private TUnion<?, ?> deserializeUnion(JsonNode node, Class<? extends TUnion> thriftClass) {
        if (!node.isObject()) {
            throw new IllegalArgumentException("Expected JSON object for thrift union: " + thriftClass.getName());
        }
        if (node.size() != 1) {
            throw new IllegalArgumentException("Union JSON must contain exactly one branch: " + thriftClass.getName());
        }
        var fieldName = node.fieldNames().next();
        var field = findFieldByName(thriftClass, fieldName);
        var fieldMetaData = getFieldMetaData(thriftClass, field);
        var declaredType = resolveFieldType(thriftClass, fieldName);
        var fieldValue = fromJsonNode(node.get(fieldName), declaredType, fieldMetaData.valueMetaData);
        var instance = newInstance(thriftClass);
        instance.setFieldValue(field, fieldValue);
        return instance;
    }

    private Collection<?> deserializeCollection(
            JsonNode node,
            Type declaredType,
            FieldValueMetaData metaData,
            Class<?> rawClass
    ) {
        if (!node.isArray()) {
            throw new IllegalArgumentException("Expected JSON array for thrift collection");
        }
        var elementType = declaredType instanceof ParameterizedType parameterizedType
                ? parameterizedType.getActualTypeArguments()[0]
                : Object.class;
        var elementMetaData = metaData instanceof CollectionMetaData collectionMetaData
                ? collectionMetaData.elemMetaData
                : null;
        Collection<Object> collection =
                Set.class.isAssignableFrom(rawClass) ? new LinkedHashSet<>() : new ArrayList<>();
        for (var elementNode : node) {
            collection.add(fromJsonNode(elementNode, elementType, elementMetaData));
        }
        return collection;
    }

    private Map<?, ?> deserializeMap(JsonNode node, Type declaredType, FieldValueMetaData metaData) {
        var keyType = declaredType instanceof ParameterizedType parameterizedType
                ? parameterizedType.getActualTypeArguments()[0]
                : Object.class;
        var valueType = declaredType instanceof ParameterizedType parameterizedType
                ? parameterizedType.getActualTypeArguments()[1]
                : Object.class;
        var keyMetaData = metaData instanceof MapMetaData mapMetaData ? mapMetaData.keyMetaData : null;
        var valueMetaData = metaData instanceof MapMetaData mapMetaData ? mapMetaData.valueMetaData : null;
        Map<Object, Object> result = new LinkedHashMap<>();
        if (node.isObject()) {
            var fields = node.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                result.put(parseObjectKey(entry.getKey(), keyType, keyMetaData),
                        fromJsonNode(entry.getValue(), valueType, valueMetaData));
            }
            return result;
        }
        if (!node.isArray()) {
            throw new IllegalArgumentException("Expected JSON object or array for thrift map");
        }
        for (var itemNode : node) {
            result.put(
                    fromJsonNode(itemNode.get("key"), keyType, keyMetaData),
                    fromJsonNode(itemNode.get("value"), valueType, valueMetaData)
            );
        }
        return result;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object deserializeEnum(JsonNode node, Class<?> rawClass) {
        if (node.isTextual()) {
            return Enum.valueOf((Class<? extends Enum>) rawClass.asSubclass(Enum.class), node.asText());
        }
        try {
            var findByValue = rawClass.getMethod("findByValue", int.class);
            return findByValue.invoke(null, node.asInt());
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalArgumentException("Failed to deserialize thrift enum: " + rawClass.getName(), ex);
        }
    }

    private byte[] readBinary(JsonNode node) {
        try {
            return node.binaryValue();
        } catch (IOException ex) {
            throw new RuntimeException("Failed to decode binary JSON value", ex);
        }
    }

    private boolean isObjectKeyCompatible(Object key) {
        return key instanceof String
                || key instanceof Number
                || key instanceof Boolean
                || key instanceof UUID
                || key instanceof TEnum;
    }

    private String stringifyMapKey(Object key) {
        if (key instanceof TEnum enumValue) {
            return ((Enum<?>) enumValue).name();
        }
        return String.valueOf(key);
    }

    private Object parseObjectKey(String key, Type keyType, FieldValueMetaData keyMetaData) {
        var rawClass = rawClassOf(keyType);
        if (String.class.equals(rawClass) || Object.class.equals(rawClass)) {
            return key;
        }
        if (Enum.class.isAssignableFrom(rawClass) && TEnum.class.isAssignableFrom(rawClass)) {
            return deserializeEnum(objectMapper.getNodeFactory().textNode(key), rawClass);
        }
        if (UUID.class.equals(rawClass)) {
            return UUID.fromString(key);
        }
        return fromJsonNode(objectMapper.getNodeFactory().textNode(key), rawClass, keyMetaData);
    }

    private Map<TFieldIdEnum, FieldMetaData> getFieldMetaDataMap(Class<?> thriftClass) {
        return FieldMetaData.getStructMetaDataMap(thriftClass.asSubclass(TBase.class));
    }

    private FieldMetaData getFieldMetaData(Class<?> thriftClass, TFieldIdEnum field) {
        var fieldMetaData = getFieldMetaDataMap(thriftClass).get(field);
        if (fieldMetaData == null) {
            throw new IllegalArgumentException(
                    "Unknown thrift field " + field.getFieldName() + " for " + thriftClass.getName());
        }
        return fieldMetaData;
    }

    private TFieldIdEnum findFieldByName(Class<?> thriftClass, String fieldName) {
        return getFieldMetaDataMap(thriftClass).keySet().stream()
                .filter(field -> field.getFieldName().equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown thrift field '" + fieldName + "' for " + thriftClass.getName()
                ));
    }

    private Type resolveFieldType(Class<?> thriftClass, String fieldName) {
        try {
            return thriftClass.getField(fieldName).getGenericType();
        } catch (NoSuchFieldException ex) {
            return resolveGetterType(thriftClass, fieldName, ex);
        }
    }

    private Type resolveGetterType(Class<?> thriftClass, String fieldName, NoSuchFieldException fieldException) {
        var getterName = "get" + toPascalCase(fieldName);
        try {
            Method getter = thriftClass.getMethod(getterName);
            return getter.getGenericReturnType();
        } catch (NoSuchMethodException ex) {
            throw new IllegalArgumentException(
                    "Failed to resolve thrift field type for " + thriftClass.getName() + "." + fieldName,
                    fieldException
            );
        }
    }

    private String toPascalCase(String fieldName) {
        var result = new StringBuilder(fieldName.length());
        var uppercaseNext = true;
        for (var ch : fieldName.toCharArray()) {
            if (ch == '_') {
                uppercaseNext = true;
                continue;
            }
            result.append(uppercaseNext ? Character.toUpperCase(ch) : ch);
            uppercaseNext = false;
        }
        return result.toString();
    }

    private Class<?> rawClassOf(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType parameterizedType) {
            return (Class<?>) parameterizedType.getRawType();
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private <T> T newInstance(Class<T> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException ex) {
            throw new IllegalArgumentException("Failed to instantiate thrift class: " + clazz.getName(), ex);
        }
    }

    private byte[] copyBinary(ByteBuffer byteBuffer) {
        var duplicate = byteBuffer.asReadOnlyBuffer();
        var bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean isSet(TBase<?, ?> value, TFieldIdEnum field) {
        return ((TBase) value).isSet(field);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object getFieldValue(TBase<?, ?> value, TFieldIdEnum field) {
        return ((TBase) value).getFieldValue(field);
    }
}
