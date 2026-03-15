package dev.vality.ccreporter.util;

import lombok.experimental.UtilityClass;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@UtilityClass
public class TimestampUtils {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;

    public static Instant parse(String value) {
        return Instant.parse(value);
    }

    public static String format(Instant value) {
        return FORMATTER.format(value);
    }

    public static LocalDateTime toLocalDateTime(Instant value) {
        return LocalDateTime.ofInstant(value, ZoneOffset.UTC);
    }

    public static LocalDateTime toOptionalLocalDateTime(Instant value) {
        return value == null ? null : LocalDateTime.ofInstant(value, ZoneOffset.UTC);
    }

    public static Instant toInstant(Timestamp timestamp) {
        return timestamp.toInstant();
    }

    public static Instant toOptionalInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
