package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.util.SearchValueNormalizer;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

import static dev.vality.ccreporter.domain.Tables.PAYMENT_TXN_CURRENT;

@Repository
@RequiredArgsConstructor
public class PaymentCurrentDao {

    private static final Field<LocalDateTime> UTC_NOW =
            DSL.field("(now() AT TIME ZONE 'utc')", LocalDateTime.class);

    private final DSLContext dslContext;

    public void upsert(PaymentTxnCurrent update) {
        var updated = dslContext.update(PAYMENT_TXN_CURRENT)
                .set(PAYMENT_TXN_CURRENT.DOMAIN_EVENT_ID, update.getDomainEventId())
                .set(PAYMENT_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT, update.getDomainEventCreatedAt())
                .set(PAYMENT_TXN_CURRENT.PARTY_ID, patchValue(update.getPartyId(), PAYMENT_TXN_CURRENT.PARTY_ID))
                .set(PAYMENT_TXN_CURRENT.SHOP_ID, patchValue(update.getShopId(), PAYMENT_TXN_CURRENT.SHOP_ID))
                .set(
                        PAYMENT_TXN_CURRENT.CREATED_AT,
                        patchValue(update.getCreatedAt(), PAYMENT_TXN_CURRENT.CREATED_AT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.FINALIZED_AT,
                        firstWriteWins(update.getFinalizedAt(), PAYMENT_TXN_CURRENT.FINALIZED_AT)
                )
                .set(PAYMENT_TXN_CURRENT.STATUS, patchValue(update.getStatus(), PAYMENT_TXN_CURRENT.STATUS))
                .set(
                        PAYMENT_TXN_CURRENT.PROVIDER_ID,
                        patchValue(update.getProviderId(), PAYMENT_TXN_CURRENT.PROVIDER_ID)
                )
                .set(
                        PAYMENT_TXN_CURRENT.TERMINAL_ID,
                        patchValue(update.getTerminalId(), PAYMENT_TXN_CURRENT.TERMINAL_ID)
                )
                .set(PAYMENT_TXN_CURRENT.AMOUNT, patchValue(update.getAmount(), PAYMENT_TXN_CURRENT.AMOUNT))
                .set(PAYMENT_TXN_CURRENT.FEE, patchValue(update.getFee(), PAYMENT_TXN_CURRENT.FEE))
                .set(PAYMENT_TXN_CURRENT.CURRENCY, patchValue(update.getCurrency(), PAYMENT_TXN_CURRENT.CURRENCY))
                .set(PAYMENT_TXN_CURRENT.TRX_ID, patchValue(update.getTrxId(), PAYMENT_TXN_CURRENT.TRX_ID))
                .set(
                        PAYMENT_TXN_CURRENT.EXTERNAL_ID,
                        patchValue(update.getExternalId(), PAYMENT_TXN_CURRENT.EXTERNAL_ID)
                )
                .set(PAYMENT_TXN_CURRENT.RRN, patchValue(update.getRrn(), PAYMENT_TXN_CURRENT.RRN))
                .set(
                        PAYMENT_TXN_CURRENT.APPROVAL_CODE,
                        patchValue(update.getApprovalCode(), PAYMENT_TXN_CURRENT.APPROVAL_CODE)
                )
                .set(
                        PAYMENT_TXN_CURRENT.PAYMENT_TOOL_TYPE,
                        patchValue(update.getPaymentToolType(), PAYMENT_TXN_CURRENT.PAYMENT_TOOL_TYPE)
                )
                .set(
                        PAYMENT_TXN_CURRENT.ERROR_SUMMARY,
                        patchValue(update.getErrorSummary(), PAYMENT_TXN_CURRENT.ERROR_SUMMARY)
                )
                .set(
                        PAYMENT_TXN_CURRENT.ORIGINAL_AMOUNT,
                        patchValue(update.getOriginalAmount(), PAYMENT_TXN_CURRENT.ORIGINAL_AMOUNT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.ORIGINAL_CURRENCY,
                        patchValue(update.getOriginalCurrency(), PAYMENT_TXN_CURRENT.ORIGINAL_CURRENCY)
                )
                .set(
                        PAYMENT_TXN_CURRENT.CONVERTED_AMOUNT,
                        patchValue(update.getConvertedAmount(), PAYMENT_TXN_CURRENT.CONVERTED_AMOUNT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.EXCHANGE_RATE_INTERNAL,
                        patchValue(update.getExchangeRateInternal(), PAYMENT_TXN_CURRENT.EXCHANGE_RATE_INTERNAL)
                )
                .set(
                        PAYMENT_TXN_CURRENT.PROVIDER_AMOUNT,
                        patchValue(update.getProviderAmount(), PAYMENT_TXN_CURRENT.PROVIDER_AMOUNT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.PROVIDER_CURRENCY,
                        patchValue(update.getProviderCurrency(), PAYMENT_TXN_CURRENT.PROVIDER_CURRENCY)
                )
                .set(PAYMENT_TXN_CURRENT.TRX_SEARCH, patchValue(trxSearch(update), PAYMENT_TXN_CURRENT.TRX_SEARCH))
                .set(PAYMENT_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                .where(PAYMENT_TXN_CURRENT.INVOICE_ID.eq(update.getInvoiceId()))
                .and(PAYMENT_TXN_CURRENT.PAYMENT_ID.eq(update.getPaymentId()))
                .and(PAYMENT_TXN_CURRENT.DOMAIN_EVENT_ID.lt(update.getDomainEventId()))
                .execute();
        if (updated > 0) {
            return;
        }
        if (!canInsert(update)) {
            return;
        }
        update.setTrxSearch(trxSearch(update));
        var record = dslContext.newRecord(PAYMENT_TXN_CURRENT, update);
        record.changed(PAYMENT_TXN_CURRENT.ID, false);
        record.changed(PAYMENT_TXN_CURRENT.UPDATED_AT, false);
        dslContext.insertInto(PAYMENT_TXN_CURRENT)
                .set(record)
                .onConflict(PAYMENT_TXN_CURRENT.INVOICE_ID, PAYMENT_TXN_CURRENT.PAYMENT_ID)
                .doNothing()
                .execute();
    }

    private String trxSearch(PaymentTxnCurrent update) {
        return SearchValueNormalizer.normalize(update.getTrxId(), update.getRrn(), update.getApprovalCode());
    }

    private boolean canInsert(PaymentTxnCurrent update) {
        return update.getPartyId() != null
                && update.getCreatedAt() != null
                && update.getStatus() != null
                && update.getAmount() != null
                && update.getCurrency() != null;
    }

    private static <T> Field<T> patchValue(T value, TableField<?, T> field) {
        return DSL.coalesce(DSL.val(value, field.getDataType()), field);
    }

    private static <T> Field<T> firstWriteWins(T value, TableField<?, T> field) {
        return DSL.coalesce(field, DSL.val(value, field.getDataType()));
    }
}
