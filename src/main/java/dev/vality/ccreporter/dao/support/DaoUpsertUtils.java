package dev.vality.ccreporter.dao.support;

import lombok.experimental.UtilityClass;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@UtilityClass
public class DaoUpsertUtils {

    public static final Field<LocalDateTime> UTC_NOW =
            DSL.field("(now() AT TIME ZONE 'utc')", LocalDateTime.class);

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Map<Field<?>, Object> buildUpsertMap(
            Table<?> table,
            Set<Field<?>> immutableFields,
            Set<Field<?>> overwriteFields,
            Map<Field<?>, Object> explicitAssignments
    ) {
        var result = new LinkedHashMap<Field<?>, Object>();
        for (var field : table.fields()) {
            if (!(field instanceof TableField<?, ?> tableField)) {
                continue;
            }
            if (immutableFields.contains(field)) {
                continue;
            }
            if (overwriteFields.contains(field)) {
                result.put(field, DSL.excluded((TableField) tableField));
            } else {
                result.put(field, DSL.coalesce(DSL.excluded((TableField) tableField), field));
            }
        }
        result.putAll(explicitAssignments);
        return result;
    }

    public static Map<Field<?>, Object> buildLookupUpsertMap(
            Table<?> table,
            Field<?> idField,
            Field<LocalDateTime> updatedAtField
    ) {
        return buildUpsertMap(
                table,
                Set.of(idField, updatedAtField),
                Arrays.stream(table.fields())
                        .filter(field -> !field.equals(idField) && !field.equals(updatedAtField))
                        .collect(Collectors.toSet()),
                Map.of(updatedAtField, UTC_NOW)
        );
    }

    public static org.jooq.Condition isIncomingEventNewer(
            Field<LocalDateTime> createdAtField,
            Field<Long> eventIdField
    ) {
        return DSL.excluded(createdAtField).gt(createdAtField)
                .or(
                        DSL.excluded(createdAtField).eq(createdAtField)
                                .and(DSL.excluded(eventIdField).gt(eventIdField))
                );
    }
}
