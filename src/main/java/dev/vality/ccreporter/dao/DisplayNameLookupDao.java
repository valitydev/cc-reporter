package dev.vality.ccreporter.dao;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

import static dev.vality.ccreporter.domain.Tables.PROVIDER_LOOKUP;
import static dev.vality.ccreporter.domain.Tables.SHOP_LOOKUP;
import static dev.vality.ccreporter.domain.Tables.TERMINAL_LOOKUP;
import static dev.vality.ccreporter.domain.Tables.WALLET_LOOKUP;

@Repository
public class DisplayNameLookupDao {

    private static final Field<LocalDateTime> UTC_NOW =
            DSL.field("(now() AT TIME ZONE 'utc')", LocalDateTime.class);

    private final DSLContext dslContext;

    public DisplayNameLookupDao(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public void upsertShop(String shopId, String shopName) {
        upsertShop(shopId, shopName, 0L);
    }

    public void upsertShop(String shopId, String shopName, long dominantVersionId) {
        if (isBlank(shopId) || isBlank(shopName)) {
            return;
        }
        upsert(
                SHOP_LOOKUP,
                SHOP_LOOKUP.SHOP_ID,
                shopId,
                SHOP_LOOKUP.SHOP_NAME,
                shopName,
                SHOP_LOOKUP.DOMINANT_VERSION_ID,
                SHOP_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    public void deleteShop(String shopId, long dominantVersionId) {
        delete(
                SHOP_LOOKUP,
                SHOP_LOOKUP.SHOP_ID,
                shopId,
                SHOP_LOOKUP.SHOP_NAME,
                SHOP_LOOKUP.DOMINANT_VERSION_ID,
                SHOP_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    public void upsertProvider(String providerId, String providerName) {
        upsertProvider(providerId, providerName, 0L);
    }

    public void upsertProvider(String providerId, String providerName, long dominantVersionId) {
        if (isBlank(providerId) || isBlank(providerName)) {
            return;
        }
        upsert(
                PROVIDER_LOOKUP,
                PROVIDER_LOOKUP.PROVIDER_ID,
                providerId,
                PROVIDER_LOOKUP.PROVIDER_NAME,
                providerName,
                PROVIDER_LOOKUP.DOMINANT_VERSION_ID,
                PROVIDER_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    public void deleteProvider(String providerId, long dominantVersionId) {
        delete(
                PROVIDER_LOOKUP,
                PROVIDER_LOOKUP.PROVIDER_ID,
                providerId,
                PROVIDER_LOOKUP.PROVIDER_NAME,
                PROVIDER_LOOKUP.DOMINANT_VERSION_ID,
                PROVIDER_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    public void upsertTerminal(String terminalId, String terminalName) {
        upsertTerminal(terminalId, terminalName, 0L);
    }

    public void upsertTerminal(String terminalId, String terminalName, long dominantVersionId) {
        if (isBlank(terminalId) || isBlank(terminalName)) {
            return;
        }
        upsert(
                TERMINAL_LOOKUP,
                TERMINAL_LOOKUP.TERMINAL_ID,
                terminalId,
                TERMINAL_LOOKUP.TERMINAL_NAME,
                terminalName,
                TERMINAL_LOOKUP.DOMINANT_VERSION_ID,
                TERMINAL_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    public void deleteTerminal(String terminalId, long dominantVersionId) {
        delete(
                TERMINAL_LOOKUP,
                TERMINAL_LOOKUP.TERMINAL_ID,
                terminalId,
                TERMINAL_LOOKUP.TERMINAL_NAME,
                TERMINAL_LOOKUP.DOMINANT_VERSION_ID,
                TERMINAL_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    public void upsertWallet(String walletId, String walletName) {
        upsertWallet(walletId, walletName, 0L);
    }

    public void upsertWallet(String walletId, String walletName, long dominantVersionId) {
        if (isBlank(walletId) || isBlank(walletName)) {
            return;
        }
        upsert(
                WALLET_LOOKUP,
                WALLET_LOOKUP.WALLET_ID,
                walletId,
                WALLET_LOOKUP.WALLET_NAME,
                walletName,
                WALLET_LOOKUP.DOMINANT_VERSION_ID,
                WALLET_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    public void deleteWallet(String walletId, long dominantVersionId) {
        delete(
                WALLET_LOOKUP,
                WALLET_LOOKUP.WALLET_ID,
                walletId,
                WALLET_LOOKUP.WALLET_NAME,
                WALLET_LOOKUP.DOMINANT_VERSION_ID,
                WALLET_LOOKUP.DELETED,
                dominantVersionId
        );
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private <R extends Record> void upsert(
            Table<R> table,
            TableField<R, String> idField,
            String id,
            TableField<R, String> nameField,
            String name,
            TableField<R, Long> versionField,
            TableField<R, Boolean> deletedField,
            long dominantVersionId
    ) {
        dslContext.insertInto(table)
                .set(idField, id)
                .set(nameField, name)
                .set(versionField, dominantVersionId)
                .set(deletedField, false)
                .onConflict(idField)
                .doUpdate()
                .set(nameField, name)
                .set(versionField, dominantVersionId)
                .set(deletedField, false)
                .set(updatedAtField(table), UTC_NOW)
                .where(versionField.le(dominantVersionId))
                .execute();
    }

    private <R extends Record> void delete(
            Table<R> table,
            TableField<R, String> idField,
            String id,
            TableField<R, String> nameField,
            TableField<R, Long> versionField,
            TableField<R, Boolean> deletedField,
            long dominantVersionId
    ) {
        if (isBlank(id)) {
            return;
        }
        dslContext.insertInto(table)
                .set(idField, id)
                .set(nameField, DSL.castNull(nameField.getDataType()))
                .set(versionField, dominantVersionId)
                .set(deletedField, true)
                .onConflict(idField)
                .doUpdate()
                .set(nameField, DSL.castNull(nameField.getDataType()))
                .set(versionField, dominantVersionId)
                .set(deletedField, true)
                .set(updatedAtField(table), UTC_NOW)
                .where(versionField.le(dominantVersionId))
                .execute();
    }

    private <R extends Record> Field<LocalDateTime> updatedAtField(Table<R> table) {
        var updatedAtField = table.field("updated_at", LocalDateTime.class);
        return updatedAtField;
    }
}
