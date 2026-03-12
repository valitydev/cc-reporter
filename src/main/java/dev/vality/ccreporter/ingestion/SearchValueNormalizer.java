package dev.vality.ccreporter.ingestion;

import java.util.Locale;
import java.util.stream.Stream;

public final class SearchValueNormalizer {

    private SearchValueNormalizer() {
    }

    public static String normalize(String... values) {
        return Stream.of(values)
                .filter(value -> value != null && !value.isBlank())
                .map(value -> value.toLowerCase(Locale.ROOT))
                .reduce((left, right) -> left + " " + right)
                .orElse(null);
    }
}
