package dev.vality.ccreporter.ingestion;

import dev.vality.fistful.cashflow.FinalCashFlowPosting;

import java.util.List;

public final class WithdrawalCashFlowExtractor {

    private WithdrawalCashFlowExtractor() {
    }

    public static Long extractFee(List<FinalCashFlowPosting> postings) {
        return postings == null
                ? null
                : postings.stream()
                .filter(posting -> posting.getSource().getAccountType().isSetWallet()
                        && posting.getDestination().getAccountType().isSetSystem())
                .map(FinalCashFlowPosting::getVolume)
                .mapToLong(dev.vality.fistful.base.Cash::getAmount)
                .sum();
    }
}
