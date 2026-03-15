package dev.vality.ccreporter.util;

import dev.vality.damsel.domain.MerchantCashFlowAccount;
import dev.vality.damsel.domain.ProviderCashFlowAccount;
import dev.vality.damsel.domain.SystemCashFlowAccount;
import lombok.experimental.UtilityClass;

import java.util.List;
import java.util.function.Predicate;

@UtilityClass
public class DomainCashFlowExtractor {

    public static Long extractPaymentAmount(List<dev.vality.damsel.domain.FinalCashFlowPosting> postings) {
        return sum(
                postings,
                posting -> posting.getSource().getAccountType().isSetProvider()
                        && posting.getSource().getAccountType().getProvider() == ProviderCashFlowAccount.settlement
                        && posting.getDestination().getAccountType().isSetMerchant()
                        && posting.getDestination().getAccountType().getMerchant() == MerchantCashFlowAccount.settlement
        );
    }

    public static Long extractPaymentFee(List<dev.vality.damsel.domain.FinalCashFlowPosting> postings) {
        return sum(
                postings,
                posting -> posting.getSource().getAccountType().isSetMerchant()
                        && posting.getSource().getAccountType().getMerchant() == MerchantCashFlowAccount.settlement
                        && posting.getDestination().getAccountType().isSetSystem()
                        && posting.getDestination().getAccountType().getSystem() == SystemCashFlowAccount.settlement
        );
    }

    public static Long extractWithdrawalFee(List<dev.vality.fistful.cashflow.FinalCashFlowPosting> postings) {
        return postings == null
                ? null
                : postings.stream()
                .filter(posting -> posting.getSource().getAccountType().isSetWallet()
                        && posting.getDestination().getAccountType().isSetSystem())
                .map(dev.vality.fistful.cashflow.FinalCashFlowPosting::getVolume)
                .mapToLong(dev.vality.fistful.base.Cash::getAmount)
                .sum();
    }

    private static Long sum(List<dev.vality.damsel.domain.FinalCashFlowPosting> postings,
                            Predicate<dev.vality.damsel.domain.FinalCashFlowPosting> filter) {
        return postings == null
                ? null
                : postings.stream()
                .filter(filter)
                .map(dev.vality.damsel.domain.FinalCashFlowPosting::getVolume)
                .mapToLong(dev.vality.damsel.domain.Cash::getAmount)
                .sum();
    }
}
