package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ingestion.SearchValueNormalizer;
import dev.vality.ccreporter.ingestion.WithdrawalCurrentUpdate;
import dev.vality.ccreporter.util.TimestampUtils;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;

import static dev.vality.ccreporter.domain.Tables.WITHDRAWAL_TXN_CURRENT;

@Repository
public class WithdrawalCurrentDao {

    private static final Field<LocalDateTime> UTC_NOW =
            DSL.field("(now() AT TIME ZONE 'utc')", LocalDateTime.class);

    private final DSLContext dslContext;

    public WithdrawalCurrentDao(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public boolean upsert(WithdrawalCurrentUpdate update) {
        if (isTransactionPatchOnly(update)) {
            var patched = dslContext.update(WITHDRAWAL_TXN_CURRENT)
                    .set(WITHDRAWAL_TXN_CURRENT.TRX_ID, patchValue(update.trxId(), WITHDRAWAL_TXN_CURRENT.TRX_ID))
                    .set(
                            WITHDRAWAL_TXN_CURRENT.TRX_SEARCH,
                            patchValue(trxSearch(update), WITHDRAWAL_TXN_CURRENT.TRX_SEARCH)
                    )
                    .set(WITHDRAWAL_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                    .where(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID.eq(update.withdrawalId()))
                    .and(
                            DSL.val(update.trxId(), WITHDRAWAL_TXN_CURRENT.TRX_ID).isNull()
                                    .or(WITHDRAWAL_TXN_CURRENT.TRX_ID.isNull())
                                    .or(WITHDRAWAL_TXN_CURRENT.TRX_ID.eq(update.trxId()))
                    )
                    .execute();
            if (patched > 0) {
                return true;
            }
        }
        var updated = dslContext.update(WITHDRAWAL_TXN_CURRENT)
                .set(WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_ID, update.domainEventId())
                .set(
                        WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT,
                        toLocalDateTime(update.domainEventCreatedAt())
                )
                .set(WITHDRAWAL_TXN_CURRENT.PARTY_ID, patchValue(update.partyId(), WITHDRAWAL_TXN_CURRENT.PARTY_ID))
                .set(WITHDRAWAL_TXN_CURRENT.WALLET_ID, patchValue(update.walletId(), WITHDRAWAL_TXN_CURRENT.WALLET_ID))
                .set(
                        WITHDRAWAL_TXN_CURRENT.WALLET_NAME,
                        patchValue(update.walletName(), WITHDRAWAL_TXN_CURRENT.WALLET_NAME)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.DESTINATION_ID,
                        patchValue(update.destinationId(), WITHDRAWAL_TXN_CURRENT.DESTINATION_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.CREATED_AT,
                        patchValue(toLocalDateTime(update.createdAt()), WITHDRAWAL_TXN_CURRENT.CREATED_AT)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.FINALIZED_AT,
                        firstWriteWins(toLocalDateTime(update.finalizedAt()), WITHDRAWAL_TXN_CURRENT.FINALIZED_AT)
                )
                .set(WITHDRAWAL_TXN_CURRENT.STATUS, patchValue(update.status(), WITHDRAWAL_TXN_CURRENT.STATUS))
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_ID,
                        patchValue(update.providerId(), WITHDRAWAL_TXN_CURRENT.PROVIDER_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_NAME,
                        patchValue(update.providerName(), WITHDRAWAL_TXN_CURRENT.PROVIDER_NAME)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_ID,
                        patchValue(update.terminalId(), WITHDRAWAL_TXN_CURRENT.TERMINAL_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_NAME,
                        patchValue(update.terminalName(), WITHDRAWAL_TXN_CURRENT.TERMINAL_NAME)
                )
                .set(WITHDRAWAL_TXN_CURRENT.AMOUNT, patchValue(update.amount(), WITHDRAWAL_TXN_CURRENT.AMOUNT))
                .set(WITHDRAWAL_TXN_CURRENT.FEE, patchValue(update.fee(), WITHDRAWAL_TXN_CURRENT.FEE))
                .set(
                        WITHDRAWAL_TXN_CURRENT.CURRENCY,
                        patchValue(update.currency(), WITHDRAWAL_TXN_CURRENT.CURRENCY)
                )
                .set(WITHDRAWAL_TXN_CURRENT.TRX_ID, patchValue(update.trxId(), WITHDRAWAL_TXN_CURRENT.TRX_ID))
                .set(
                        WITHDRAWAL_TXN_CURRENT.EXTERNAL_ID,
                        patchValue(update.externalId(), WITHDRAWAL_TXN_CURRENT.EXTERNAL_ID)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ERROR_CODE,
                        patchValue(update.errorCode(), WITHDRAWAL_TXN_CURRENT.ERROR_CODE)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ERROR_REASON,
                        patchValue(update.errorReason(), WITHDRAWAL_TXN_CURRENT.ERROR_REASON)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ERROR_SUB_FAILURE,
                        patchValue(update.errorSubFailure(), WITHDRAWAL_TXN_CURRENT.ERROR_SUB_FAILURE)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_AMOUNT,
                        patchValue(update.originalAmount(), WITHDRAWAL_TXN_CURRENT.ORIGINAL_AMOUNT)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_CURRENCY,
                        patchValue(update.originalCurrency(), WITHDRAWAL_TXN_CURRENT.ORIGINAL_CURRENCY)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.CONVERTED_AMOUNT,
                        patchValue(update.convertedAmount(), WITHDRAWAL_TXN_CURRENT.CONVERTED_AMOUNT)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.EXCHANGE_RATE_INTERNAL,
                        patchValue(update.exchangeRateInternal(), WITHDRAWAL_TXN_CURRENT.EXCHANGE_RATE_INTERNAL)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_AMOUNT,
                        patchValue(update.providerAmount(), WITHDRAWAL_TXN_CURRENT.PROVIDER_AMOUNT)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_CURRENCY,
                        patchValue(update.providerCurrency(), WITHDRAWAL_TXN_CURRENT.PROVIDER_CURRENCY)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.WALLET_SEARCH,
                        patchValue(walletSearch(update), WITHDRAWAL_TXN_CURRENT.WALLET_SEARCH)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_SEARCH,
                        patchValue(providerSearch(update), WITHDRAWAL_TXN_CURRENT.PROVIDER_SEARCH)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_SEARCH,
                        patchValue(terminalSearch(update), WITHDRAWAL_TXN_CURRENT.TERMINAL_SEARCH)
                )
                .set(
                        WITHDRAWAL_TXN_CURRENT.TRX_SEARCH,
                        patchValue(trxSearch(update), WITHDRAWAL_TXN_CURRENT.TRX_SEARCH)
                )
                .set(WITHDRAWAL_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                .where(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID.eq(update.withdrawalId()))
                .and(WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_ID.lt(update.domainEventId()))
                .execute();
        if (updated > 0) {
            return true;
        }
        if (!canInsert(update)) {
            return false;
        }
        var inserted = dslContext.insertInto(
                        WITHDRAWAL_TXN_CURRENT,
                        WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID,
                        WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_ID,
                        WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT,
                        WITHDRAWAL_TXN_CURRENT.PARTY_ID,
                        WITHDRAWAL_TXN_CURRENT.WALLET_ID,
                        WITHDRAWAL_TXN_CURRENT.WALLET_NAME,
                        WITHDRAWAL_TXN_CURRENT.DESTINATION_ID,
                        WITHDRAWAL_TXN_CURRENT.CREATED_AT,
                        WITHDRAWAL_TXN_CURRENT.FINALIZED_AT,
                        WITHDRAWAL_TXN_CURRENT.STATUS,
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_ID,
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_NAME,
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_ID,
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_NAME,
                        WITHDRAWAL_TXN_CURRENT.AMOUNT,
                        WITHDRAWAL_TXN_CURRENT.FEE,
                        WITHDRAWAL_TXN_CURRENT.CURRENCY,
                        WITHDRAWAL_TXN_CURRENT.TRX_ID,
                        WITHDRAWAL_TXN_CURRENT.EXTERNAL_ID,
                        WITHDRAWAL_TXN_CURRENT.ERROR_CODE,
                        WITHDRAWAL_TXN_CURRENT.ERROR_REASON,
                        WITHDRAWAL_TXN_CURRENT.ERROR_SUB_FAILURE,
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_AMOUNT,
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_CURRENCY,
                        WITHDRAWAL_TXN_CURRENT.CONVERTED_AMOUNT,
                        WITHDRAWAL_TXN_CURRENT.EXCHANGE_RATE_INTERNAL,
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_AMOUNT,
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_CURRENCY,
                        WITHDRAWAL_TXN_CURRENT.WALLET_SEARCH,
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_SEARCH,
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_SEARCH,
                        WITHDRAWAL_TXN_CURRENT.TRX_SEARCH
                )
                .values(
                        update.withdrawalId(),
                        update.domainEventId(),
                        toLocalDateTime(update.domainEventCreatedAt()),
                        update.partyId(),
                        update.walletId(),
                        update.walletName(),
                        update.destinationId(),
                        toLocalDateTime(update.createdAt()),
                        toLocalDateTime(update.finalizedAt()),
                        update.status(),
                        update.providerId(),
                        update.providerName(),
                        update.terminalId(),
                        update.terminalName(),
                        update.amount(),
                        update.fee(),
                        update.currency(),
                        update.trxId(),
                        update.externalId(),
                        update.errorCode(),
                        update.errorReason(),
                        update.errorSubFailure(),
                        update.originalAmount(),
                        update.originalCurrency(),
                        update.convertedAmount(),
                        update.exchangeRateInternal(),
                        update.providerAmount(),
                        update.providerCurrency(),
                        walletSearch(update),
                        providerSearch(update),
                        terminalSearch(update),
                        trxSearch(update)
                )
                .onConflict(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID)
                .doNothing()
                .execute();
        return inserted > 0;
    }

    private LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }

    private String walletSearch(WithdrawalCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.walletId(), update.walletName());
    }

    private String providerSearch(WithdrawalCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.providerId(), update.providerName());
    }

    private String terminalSearch(WithdrawalCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.terminalId(), update.terminalName());
    }

    private String trxSearch(WithdrawalCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.trxId());
    }

    private boolean canInsert(WithdrawalCurrentUpdate update) {
        return update.partyId() != null
                && update.createdAt() != null
                && update.status() != null
                && update.amount() != null
                && update.currency() != null;
    }

    private boolean isTransactionPatchOnly(WithdrawalCurrentUpdate update) {
        return update.trxId() != null
                && update.partyId() == null
                && update.walletId() == null
                && update.walletName() == null
                && update.destinationId() == null
                && update.createdAt() == null
                && update.finalizedAt() == null
                && update.status() == null
                && update.providerId() == null
                && update.providerName() == null
                && update.terminalId() == null
                && update.terminalName() == null
                && update.amount() == null
                && update.fee() == null
                && update.currency() == null
                && update.externalId() == null
                && update.errorCode() == null
                && update.errorReason() == null
                && update.errorSubFailure() == null
                && update.originalAmount() == null
                && update.originalCurrency() == null
                && update.convertedAmount() == null
                && update.exchangeRateInternal() == null
                && update.providerAmount() == null
                && update.providerCurrency() == null;
    }

    private static <T> Field<T> patchValue(T value, TableField<?, T> field) {
        return DSL.coalesce(DSL.val(value, field.getDataType()), field);
    }

    private static <T> Field<T> firstWriteWins(T value, TableField<?, T> field) {
        return DSL.coalesce(field, DSL.val(value, field.getDataType()));
    }
}
