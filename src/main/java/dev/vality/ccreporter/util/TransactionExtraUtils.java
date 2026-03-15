package dev.vality.ccreporter.util;

import dev.vality.damsel.domain.TransactionInfo;
import lombok.experimental.UtilityClass;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@UtilityClass
public class TransactionExtraUtils {

    public static final String CONVERTED_AMOUNT_KEY = "converted_amount";
    public static final String EXCHANGE_RATE_KEY = "_rate";

    public static Long getConvertedAmount(TransactionInfo trx) {
        return getValue(trx, CONVERTED_AMOUNT_KEY, Long::parseLong);
    }

    public static BigDecimal getExchangeRate(TransactionInfo trx) {
        return getValue(trx, EXCHANGE_RATE_KEY, BigDecimal::new);
    }

    public static <T> T getValue(TransactionInfo trx, String keySuffix, Function<String, T> parser) {
        if (trx == null || trx.getExtra() == null || trx.getExtra().isEmpty()) {
            return null;
        }
        return trx.getExtra().entrySet().stream()
                .filter(entry -> matchesKey(entry.getKey(), keySuffix))
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .map(Map.Entry::getValue)
                .map(value -> parseValue(value, parser))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElse(null);
    }

    public static boolean matchesKey(String key, String keySuffix) {
        return key != null && (key.equals(keySuffix) || key.endsWith(keySuffix) || key.contains(keySuffix));
    }

    public static <T> Optional<T> parseValue(String value, Function<String, T> parser) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(parser.apply(value));
        } catch (RuntimeException ex) {
            return Optional.empty();
        }
    }
}
