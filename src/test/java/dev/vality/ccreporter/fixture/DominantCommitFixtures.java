package dev.vality.ccreporter.fixture;

import dev.vality.damsel.domain.*;
import dev.vality.damsel.domain_config_v2.*;

import java.util.List;

public final class DominantCommitFixtures {

    private DominantCommitFixtures() {
    }

    public static HistoricalCommit insertCommit(long version) {
        return commit(
                version,
                List.of(
                        FinalOperation.insert(new FinalInsertOp(shopObject("shop-lookup", "Lookup Shop"))),
                        FinalOperation.insert(new FinalInsertOp(providerObject(1001, "Lookup Provider"))),
                        FinalOperation.insert(new FinalInsertOp(terminalObject(1002, "Lookup Terminal"))),
                        FinalOperation.insert(new FinalInsertOp(walletObject("wallet-lookup", "Lookup Wallet")))
                )
        );
    }

    public static HistoricalCommit updateShopCommit(long version, String name) {
        return commit(version, List.of(FinalOperation.update(new UpdateOp(shopObject("shop-lookup", name)))));
    }

    public static HistoricalCommit removeShopCommit(long version) {
        return commit(version, List.of(FinalOperation.remove(new RemoveOp(shopReference("shop-lookup")))));
    }

    public static HistoricalCommit updateProviderCommit(long version, String name) {
        return commit(version, List.of(FinalOperation.update(new UpdateOp(providerObject(1001, name)))));
    }

    private static HistoricalCommit commit(long version, List<FinalOperation> operations) {
        return new HistoricalCommit()
                .setVersion(version)
                .setCreatedAt("2026-01-01T00:00:00")
                .setChangedBy(new Author().setId("dominant").setName("dominant").setEmail("dominant@test"))
                .setOps(operations);
    }

    private static DomainObject shopObject(String shopId, String name) {
        return DomainObject.shop_config(
                new ShopConfigObject()
                        .setRef(new ShopConfigRef().setId(shopId))
                        .setData(new ShopConfig().setName(name))
        );
    }

    private static DomainObject providerObject(int providerId, String name) {
        return DomainObject.provider(
                new ProviderObject()
                        .setRef(new ProviderRef().setId(providerId))
                        .setData(new Provider().setName(name))
        );
    }

    private static DomainObject terminalObject(int terminalId, String name) {
        return DomainObject.terminal(
                new TerminalObject()
                        .setRef(new TerminalRef().setId(terminalId))
                        .setData(new Terminal().setName(name))
        );
    }

    private static DomainObject walletObject(String walletId, String name) {
        return DomainObject.wallet_config(
                new WalletConfigObject()
                        .setRef(new WalletConfigRef().setId(walletId))
                        .setData(new WalletConfig().setName(name))
        );
    }

    private static Reference shopReference(String shopId) {
        return Reference.shop_config(new ShopConfigRef().setId(shopId));
    }
}
