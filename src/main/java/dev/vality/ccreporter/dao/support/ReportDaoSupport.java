package dev.vality.ccreporter.dao.support;

import lombok.experimental.UtilityClass;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;

@UtilityClass
public class ReportDaoSupport {

    public static <T> Field<T> firstValueOrExisting(T value, Field<T> field) {
        return DSL.coalesce(field, DSL.val(value, field));
    }

    public static Field<LocalDateTime> firstTimestampOrExisting(Instant value, Field<LocalDateTime> field) {
        return firstValueOrExisting(toLocalDateTime(value), field);
    }

    public static Condition isReadyAt(Field<LocalDateTime> field, Instant now) {
        return field.isNull().or(field.le(timestampValue(now, field)));
    }

    public static Field<LocalDateTime> timestampValue(Instant value, Field<LocalDateTime> field) {
        if (value == null) {
            return DSL.castNull(field.getDataType());
        }
        return DSL.val(Timestamp.from(value)).cast(field.getDataType());
    }

    private static LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : Timestamp.from(value).toLocalDateTime();
    }
}
