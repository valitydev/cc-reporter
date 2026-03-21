package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.domain.tables.pojos.ProviderLookup;
import dev.vality.ccreporter.domain.tables.pojos.ShopLookup;
import dev.vality.ccreporter.domain.tables.pojos.TerminalLookup;
import dev.vality.ccreporter.domain.tables.pojos.WalletLookup;
import lombok.RequiredArgsConstructor;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

import static dev.vality.ccreporter.domain.Tables.*;
import static dev.vality.ccreporter.util.DaoUpsertUtils.buildLookupUpsertMap;
import static dev.vality.ccreporter.util.SearchValueNormalizer.normalize;

@Repository
@RequiredArgsConstructor
public class DisplayNameLookupDao {

    private final DSLContext dslContext;

    public void upsert(LookupType lookupType, String id, String name, long dominantVersionId, boolean deleted) {
        switch (lookupType) {
            case SHOP -> insertShop(id, name, dominantVersionId, deleted);
            case PROVIDER -> insertProvider(id, name, dominantVersionId, deleted);
            case TERMINAL -> insertTerminal(id, name, dominantVersionId, deleted);
            case WALLET -> insertWallet(id, name, dominantVersionId, deleted);
        }
    }

    private void insertShop(String shopId, String shopName, long dominantVersionId, boolean deleted) {
        if (isBlank(shopId)) {
            return;
        }
        upsert(
                SHOP_LOOKUP,
                SHOP_LOOKUP.SHOP_ID,
                SHOP_LOOKUP.DOMINANT_VERSION_ID,
                SHOP_LOOKUP.UPDATED_AT,
                new ShopLookup()
                        .setShopId(shopId)
                        .setShopName(shopName)
                        .setShopSearch(searchValue(shopId, shopName))
                        .setDominantVersionId(dominantVersionId)
                        .setDeleted(deleted)
        );
    }

    private void insertProvider(String providerId, String providerName, long dominantVersionId, boolean deleted) {
        if (isBlank(providerId)) {
            return;
        }
        upsert(
                PROVIDER_LOOKUP,
                PROVIDER_LOOKUP.PROVIDER_ID,
                PROVIDER_LOOKUP.DOMINANT_VERSION_ID,
                PROVIDER_LOOKUP.UPDATED_AT,
                new ProviderLookup()
                        .setProviderId(providerId)
                        .setProviderName(providerName)
                        .setProviderSearch(searchValue(providerId, providerName))
                        .setDominantVersionId(dominantVersionId)
                        .setDeleted(deleted)
        );
    }

    private void insertTerminal(String terminalId, String terminalName, long dominantVersionId, boolean deleted) {
        if (isBlank(terminalId)) {
            return;
        }
        upsert(
                TERMINAL_LOOKUP,
                TERMINAL_LOOKUP.TERMINAL_ID,
                TERMINAL_LOOKUP.DOMINANT_VERSION_ID,
                TERMINAL_LOOKUP.UPDATED_AT,
                new TerminalLookup()
                        .setTerminalId(terminalId)
                        .setTerminalName(terminalName)
                        .setTerminalSearch(searchValue(terminalId, terminalName))
                        .setDominantVersionId(dominantVersionId)
                        .setDeleted(deleted)
        );
    }

    private void insertWallet(String walletId, String walletName, long dominantVersionId, boolean deleted) {
        if (isBlank(walletId)) {
            return;
        }
        upsert(
                WALLET_LOOKUP,
                WALLET_LOOKUP.WALLET_ID,
                WALLET_LOOKUP.DOMINANT_VERSION_ID,
                WALLET_LOOKUP.UPDATED_AT,
                new WalletLookup()
                        .setWalletId(walletId)
                        .setWalletName(walletName)
                        .setWalletSearch(searchValue(walletId, walletName))
                        .setDominantVersionId(dominantVersionId)
                        .setDeleted(deleted)
        );
    }

    private <R extends Record, P> void upsert(
            Table<R> table,
            TableField<R, String> idField,
            TableField<R, Long> versionField,
            Field<LocalDateTime> updatedAtField,
            P update
    ) {
        var record = dslContext.newRecord(table, update);
        record.changed(updatedAtField, false);

        dslContext.insertInto(table)
                .set(record)
                .onConflict(idField)
                .doUpdate()
                .set(buildLookupUpsertMap(table, idField, updatedAtField))
                .where(versionField.le(DSL.excluded(versionField)))
                .execute();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private String searchValue(String id, String name) {
        return isBlank(name) ? normalize(id) : normalize(id, name);
    }

    public enum LookupType {
        SHOP,
        PROVIDER,
        TERMINAL,
        WALLET
    }
}
