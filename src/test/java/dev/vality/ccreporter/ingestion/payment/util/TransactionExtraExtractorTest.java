package dev.vality.ccreporter.ingestion.payment.util;

import dev.vality.damsel.domain.TransactionInfo;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionExtraExtractorTest {

    @Test
    void extractsCurrenciesAmountAndMatchingRateFromSameFxPair() {
        var transaction = new TransactionInfo()
                .setId("trx-1")
                .setExtra(Map.of(
                        "usd_to_jpy_converted_amount", "1234",
                        "usd_to_jpy_rate", "156.2500000000",
                        "rub_to_eur_rate", "0.0100000000"
                ));

        var conversion = TransactionExtraExtractor.extractFxConversion(transaction).orElseThrow();

        assertThat(conversion.originalCurrency()).isEqualTo("USD");
        assertThat(conversion.convertedCurrency()).isEqualTo("JPY");
        assertThat(conversion.convertedAmount()).isEqualTo(1234L);
        assertThat(conversion.exchangeRate()).isEqualByComparingTo(new BigDecimal("156.2500000000"));
    }

    @Test
    void ignoresMalformedAndUnqualifiedConvertedAmountKeys() {
        var transaction = new TransactionInfo()
                .setId("trx-1")
                .setExtra(Map.of(
                        "converted_amount", "100",
                        "rub_to_eur_converted_amount", "not-a-number"
                ));

        assertThat(TransactionExtraExtractor.extractFxConversion(transaction)).isEmpty();
    }
}
