package dev.vality.ccreporter.ingestion.shared.cashflow;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CashFlowAmountExtractorTest {

    @Test
    void returnsNullWhenPaymentPostingsDoNotContainRequestedAmounts() {
        assertThat(CashFlowAmountExtractor.extractPaymentAmount(List.of())).isNull();
        assertThat(CashFlowAmountExtractor.extractPaymentFee(List.of())).isNull();
    }

    @Test
    void returnsNullWhenWithdrawalPostingsDoNotContainFee() {
        assertThat(CashFlowAmountExtractor.extractWithdrawalFee(List.of())).isNull();
    }
}
