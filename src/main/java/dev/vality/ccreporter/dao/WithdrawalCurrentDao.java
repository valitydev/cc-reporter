package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.ccreporter.util.SearchValueNormalizer;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

import static dev.vality.ccreporter.domain.Tables.WITHDRAWAL_TXN_CURRENT;

@Repository
@RequiredArgsConstructor
public class WithdrawalCurrentDao {

    private static final Field<LocalDateTime> UTC_NOW =
            DSL.field("(now() AT TIME ZONE 'utc')", LocalDateTime.class);

    private final DSLContext dslContext;

    public void upsert(WithdrawalTxnCurrent update) {
        if (isTransactionPatchOnly(update)) {
            var patched = dslContext.update(WITHDRAWAL_TXN_CURRENT)
                    .set(WITHDRAWAL_TXN_CURRENT.TRX_ID, patchValue(update.getTrxId(), WITHDRAWAL_TXN_CURRENT.TRX_ID))
                    .set(
                            WITHDRAWAL_TXN_CURRENT.TRX_SEARCH,
                            patchValue(trxSearch(update), WITHDRAWAL_TXN_CURRENT.TRX_SEARCH)
                    )
                    .set(WITHDRAWAL_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                    .where(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID.eq(update.getWithdrawalId()))
                    .and(
                            DSL.val(update.getTrxId(), WITHDRAWAL_TXN_CURRENT.TRX_ID).isNull()
                                    .or(WITHDRAWAL_TXN_CURRENT.TRX_ID.isNull())
                                    .or(WITHDRAWAL_TXN_CURRENT.TRX_ID.eq(update.getTrxId()))
                    )
                    .execute();
            if (patched > 0) {
                return;
            }
        }
        var updated = dslContext.update(WITHDRAWAL_TXN_CURRENT)
                .set(WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_ID, update.getDomainEventId())
                .set(
                        WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT,
                        update.getDomainEventCreatedAt()
                )
                .set(WITHDRAWAL_TXN_CURRENT.PARTY_ID, patchValue(update.getPartyId(), WITHDRAWAL_TXN_CURRENT.PARTY_ID))
                .set(
                        WITHDRAWAL_TXN_CURRENT.WALLET_ID,
                        patchValue(update.getWalletId(), WITHDRAWAL_TXN_CURRENT.WALLET_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.DESTINATION_ID,
                        patchValue(update.getDestinationId(), WITHDRAWAL_TXN_CURRENT.DESTINATION_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.CREATED_AT,
                        patchValue(update.getCreatedAt(), WITHDRAWAL_TXN_CURRENT.CREATED_AT)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.FINALIZED_AT,
                        firstWriteWins(update.getFinalizedAt(), WITHDRAWAL_TXN_CURRENT.FINALIZED_AT)
                )
                .set(WITHDRAWAL_TXN_CURRENT.STATUS, patchValue(update.getStatus(), WITHDRAWAL_TXN_CURRENT.STATUS))
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_ID,
                        patchValue(update.getProviderId(), WITHDRAWAL_TXN_CURRENT.PROVIDER_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_ID,
                        patchValue(update.getTerminalId(), WITHDRAWAL_TXN_CURRENT.TERMINAL_ID)
                )
                .set(WITHDRAWAL_TXN_CURRENT.AMOUNT, patchValue(update.getAmount(), WITHDRAWAL_TXN_CURRENT.AMOUNT))
                .set(WITHDRAWAL_TXN_CURRENT.FEE, patchValue(update.getFee(), WITHDRAWAL_TXN_CURRENT.FEE))
                .set(
                        WITHDRAWAL_TXN_CURRENT.CURRENCY,
                        patchValue(update.getCurrency(), WITHDRAWAL_TXN_CURRENT.CURRENCY)
                )
                .set(WITHDRAWAL_TXN_CURRENT.TRX_ID, patchValue(update.getTrxId(), WITHDRAWAL_TXN_CURRENT.TRX_ID))
                .set(
                        WITHDRAWAL_TXN_CURRENT.EXTERNAL_ID,
                        patchValue(update.getExternalId(), WITHDRAWAL_TXN_CURRENT.EXTERNAL_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ERROR_SUMMARY,
                        patchValue(update.getErrorSummary(), WITHDRAWAL_TXN_CURRENT.ERROR_SUMMARY)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_AMOUNT,
                        patchValue(update.getOriginalAmount(), WITHDRAWAL_TXN_CURRENT.ORIGINAL_AMOUNT)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_CURRENCY,
                        patchValue(update.getOriginalCurrency(), WITHDRAWAL_TXN_CURRENT.ORIGINAL_CURRENCY)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.EXCHANGE_RATE_INTERNAL,
                        patchValue(update.getExchangeRateInternal(), WITHDRAWAL_TXN_CURRENT.EXCHANGE_RATE_INTERNAL)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_AMOUNT,
                        patchValue(update.getProviderAmount(), WITHDRAWAL_TXN_CURRENT.PROVIDER_AMOUNT)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_CURRENCY,
                        patchValue(update.getProviderCurrency(), WITHDRAWAL_TXN_CURRENT.PROVIDER_CURRENCY)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.TRX_SEARCH,
                        patchValue(trxSearch(update), WITHDRAWAL_TXN_CURRENT.TRX_SEARCH)
                )
                .set(WITHDRAWAL_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                .where(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID.eq(update.getWithdrawalId()))
                .and(WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_ID.lt(update.getDomainEventId()))
                .execute();
        if (updated > 0) {
            return;
        }
        if (!canInsert(update)) {
            return;
        }
        update.setTrxSearch(trxSearch(update));
        var record = dslContext.newRecord(WITHDRAWAL_TXN_CURRENT, update);
        record.changed(WITHDRAWAL_TXN_CURRENT.ID, false);
        record.changed(WITHDRAWAL_TXN_CURRENT.UPDATED_AT, false);
        dslContext.insertInto(WITHDRAWAL_TXN_CURRENT)
                .set(record)
                .onConflict(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID)
                .doNothing()
                .execute();
    }

    private String trxSearch(WithdrawalTxnCurrent update) {
        return SearchValueNormalizer.normalize(update.getTrxId());
    }

    private boolean canInsert(WithdrawalTxnCurrent update) {
        return update.getPartyId() != null
                && update.getCreatedAt() != null
                && update.getStatus() != null
                && update.getAmount() != null
                && update.getCurrency() != null;
    }

    private boolean isTransactionPatchOnly(WithdrawalTxnCurrent update) {
        return update.getTrxId() != null
                && update.getPartyId() == null
                && update.getWalletId() == null
                && update.getDestinationId() == null
                && update.getCreatedAt() == null
                && update.getFinalizedAt() == null
                && update.getStatus() == null
                && update.getProviderId() == null
                && update.getTerminalId() == null
                && update.getAmount() == null
                && update.getFee() == null
                && update.getCurrency() == null
                && update.getExternalId() == null
                && update.getErrorSummary() == null
                && update.getOriginalAmount() == null
                && update.getOriginalCurrency() == null
                && update.getExchangeRateInternal() == null
                && update.getProviderAmount() == null
                && update.getProviderCurrency() == null;
    }

    private static <T> Field<T> patchValue(T value, TableField<?, T> field) {
        return DSL.coalesce(DSL.val(value, field.getDataType()), field);
    }

    private static <T> Field<T> firstWriteWins(T value, TableField<?, T> field) {
        return DSL.coalesce(field, DSL.val(value, field.getDataType()));
    }
}
