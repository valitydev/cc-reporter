package dev.vality.ccreporter.ingestion.payment.util;

import dev.vality.damsel.domain.TransactionInfo;
import lombok.experimental.UtilityClass;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

@UtilityClass
public class TransactionExtraExtractor {

    private static final Pattern CONVERTED_AMOUNT_KEY = Pattern.compile(
            "^([a-zA-Z]{3})_to_([a-zA-Z]{3})_converted_amount$"
    );

    public static Optional<FxConversion> extractFxConversion(TransactionInfo trx) {
        if (trx == null || trx.getExtra() == null || trx.getExtra().isEmpty()) {
            return Optional.empty();
        }
        return trx.getExtra().entrySet().stream()
                .sorted(java.util.Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .map(entry -> parseFxConversion(entry.getKey(), entry.getValue(), trx))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private static Optional<FxConversion> parseFxConversion(String key, String amountValue, TransactionInfo trx) {
        if (key == null) {
            return Optional.empty();
        }
        var matcher = CONVERTED_AMOUNT_KEY.matcher(key);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        var convertedAmount = parseValue(amountValue, Long::parseLong);
        if (convertedAmount.isEmpty()) {
            return Optional.empty();
        }
        var rateKey = key.substring(0, key.length() - "converted_amount".length()) + "rate";
        var exchangeRate = parseValue(trx.getExtra().get(rateKey), BigDecimal::new).orElse(null);
        return Optional.of(new FxConversion(
                matcher.group(1).toUpperCase(java.util.Locale.ROOT),
                matcher.group(2).toUpperCase(java.util.Locale.ROOT),
                convertedAmount.get(),
                exchangeRate
        ));
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

    public record FxConversion(
            String originalCurrency,
            String convertedCurrency,
            long convertedAmount,
            BigDecimal exchangeRate
    ) {
    }
}
