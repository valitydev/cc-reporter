package dev.vality.ccreporter.ingestion.payment;

import dev.vality.damsel.domain.FinalCashFlowPosting;
import dev.vality.damsel.domain.MerchantCashFlowAccount;
import dev.vality.damsel.domain.ProviderCashFlowAccount;

import java.util.List;

public final class PaymentCashFlowExtractor {

    private PaymentCashFlowExtractor() {
    }

    public static Long extractAmount(List<FinalCashFlowPosting> postings) {
        return sum(
                postings,
                posting -> posting.getSource().getAccountType().isSetProvider()
                        && posting.getSource().getAccountType().getProvider() == ProviderCashFlowAccount.settlement
                        && posting.getDestination().getAccountType().isSetMerchant()
                        && posting.getDestination().getAccountType().getMerchant() == MerchantCashFlowAccount.settlement
        );
    }

    public static Long extractFee(List<FinalCashFlowPosting> postings) {
        return sum(
                postings,
                posting -> posting.getSource().getAccountType().isSetMerchant()
                        && posting.getSource().getAccountType().getMerchant() == MerchantCashFlowAccount.settlement
                        && posting.getDestination().getAccountType().isSetSystem()
        );
    }

    private static Long sum(List<FinalCashFlowPosting> postings,
                            java.util.function.Predicate<FinalCashFlowPosting> filter) {
        return postings == null
                ? null
                : postings.stream()
                .filter(filter)
                .map(FinalCashFlowPosting::getVolume)
                .mapToLong(dev.vality.damsel.domain.Cash::getAmount)
                .sum();
    }
}
