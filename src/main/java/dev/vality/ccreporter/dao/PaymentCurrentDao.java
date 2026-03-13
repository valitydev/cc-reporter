package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ingestion.PaymentCurrentUpdate;
import dev.vality.ccreporter.ingestion.SearchValueNormalizer;
import dev.vality.ccreporter.util.TimestampUtils;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;

import static dev.vality.ccreporter.domain.Tables.PAYMENT_TXN_CURRENT;

@Repository
public class PaymentCurrentDao {

    private static final Field<LocalDateTime> UTC_NOW =
            DSL.field("(now() AT TIME ZONE 'utc')", LocalDateTime.class);

    private final DSLContext dslContext;

    public PaymentCurrentDao(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public boolean upsert(PaymentCurrentUpdate update) {
        var updated = dslContext.update(PAYMENT_TXN_CURRENT)
                .set(PAYMENT_TXN_CURRENT.DOMAIN_EVENT_ID, update.domainEventId())
                .set(PAYMENT_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT, toLocalDateTime(update.domainEventCreatedAt()))
                .set(PAYMENT_TXN_CURRENT.PARTY_ID, patchValue(update.partyId(), PAYMENT_TXN_CURRENT.PARTY_ID))
                .set(PAYMENT_TXN_CURRENT.SHOP_ID, patchValue(update.shopId(), PAYMENT_TXN_CURRENT.SHOP_ID))
                .set(PAYMENT_TXN_CURRENT.SHOP_NAME, patchValue(update.shopName(), PAYMENT_TXN_CURRENT.SHOP_NAME))
                .set(
                        PAYMENT_TXN_CURRENT.CREATED_AT,
                        patchValue(toLocalDateTime(update.createdAt()), PAYMENT_TXN_CURRENT.CREATED_AT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.FINALIZED_AT,
                        firstWriteWins(toLocalDateTime(update.finalizedAt()), PAYMENT_TXN_CURRENT.FINALIZED_AT)
                )
                .set(PAYMENT_TXN_CURRENT.STATUS, patchValue(update.status(), PAYMENT_TXN_CURRENT.STATUS))
                .set(PAYMENT_TXN_CURRENT.PROVIDER_ID, patchValue(update.providerId(), PAYMENT_TXN_CURRENT.PROVIDER_ID))
                .set(
                        PAYMENT_TXN_CURRENT.PROVIDER_NAME,
                        patchValue(update.providerName(), PAYMENT_TXN_CURRENT.PROVIDER_NAME)
                )
                .set(PAYMENT_TXN_CURRENT.TERMINAL_ID, patchValue(update.terminalId(), PAYMENT_TXN_CURRENT.TERMINAL_ID))
                .set(
                        PAYMENT_TXN_CURRENT.TERMINAL_NAME,
                        patchValue(update.terminalName(), PAYMENT_TXN_CURRENT.TERMINAL_NAME)
                )
                .set(PAYMENT_TXN_CURRENT.AMOUNT, patchValue(update.amount(), PAYMENT_TXN_CURRENT.AMOUNT))
                .set(PAYMENT_TXN_CURRENT.FEE, patchValue(update.fee(), PAYMENT_TXN_CURRENT.FEE))
                .set(PAYMENT_TXN_CURRENT.CURRENCY, patchValue(update.currency(), PAYMENT_TXN_CURRENT.CURRENCY))
                .set(PAYMENT_TXN_CURRENT.TRX_ID, patchValue(update.trxId(), PAYMENT_TXN_CURRENT.TRX_ID))
                .set(PAYMENT_TXN_CURRENT.EXTERNAL_ID, patchValue(update.externalId(), PAYMENT_TXN_CURRENT.EXTERNAL_ID))
                .set(PAYMENT_TXN_CURRENT.RRN, patchValue(update.rrn(), PAYMENT_TXN_CURRENT.RRN))
                .set(
                        PAYMENT_TXN_CURRENT.APPROVAL_CODE,
                        patchValue(update.approvalCode(), PAYMENT_TXN_CURRENT.APPROVAL_CODE)
                )
                .set(
                        PAYMENT_TXN_CURRENT.PAYMENT_TOOL_TYPE,
                        patchValue(update.paymentToolType(), PAYMENT_TXN_CURRENT.PAYMENT_TOOL_TYPE)
                )
                .set(
                        PAYMENT_TXN_CURRENT.ORIGINAL_AMOUNT,
                        patchValue(update.originalAmount(), PAYMENT_TXN_CURRENT.ORIGINAL_AMOUNT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.ORIGINAL_CURRENCY,
                        patchValue(update.originalCurrency(), PAYMENT_TXN_CURRENT.ORIGINAL_CURRENCY)
                )
                .set(
                        PAYMENT_TXN_CURRENT.CONVERTED_AMOUNT,
                        patchValue(update.convertedAmount(), PAYMENT_TXN_CURRENT.CONVERTED_AMOUNT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.EXCHANGE_RATE_INTERNAL,
                        patchValue(update.exchangeRateInternal(), PAYMENT_TXN_CURRENT.EXCHANGE_RATE_INTERNAL)
                )
                .set(
                        PAYMENT_TXN_CURRENT.PROVIDER_AMOUNT,
                        patchValue(update.providerAmount(), PAYMENT_TXN_CURRENT.PROVIDER_AMOUNT)
                )
                .set(
                        PAYMENT_TXN_CURRENT.PROVIDER_CURRENCY,
                        patchValue(update.providerCurrency(), PAYMENT_TXN_CURRENT.PROVIDER_CURRENCY)
                )
                .set(PAYMENT_TXN_CURRENT.SHOP_SEARCH, patchValue(shopSearch(update), PAYMENT_TXN_CURRENT.SHOP_SEARCH))
                .set(
                        PAYMENT_TXN_CURRENT.PROVIDER_SEARCH,
                        patchValue(providerSearch(update), PAYMENT_TXN_CURRENT.PROVIDER_SEARCH)
                )
                .set(
                        PAYMENT_TXN_CURRENT.TERMINAL_SEARCH,
                        patchValue(terminalSearch(update), PAYMENT_TXN_CURRENT.TERMINAL_SEARCH)
                )
                .set(PAYMENT_TXN_CURRENT.TRX_SEARCH, patchValue(trxSearch(update), PAYMENT_TXN_CURRENT.TRX_SEARCH))
                .set(PAYMENT_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                .where(PAYMENT_TXN_CURRENT.INVOICE_ID.eq(update.invoiceId()))
                .and(PAYMENT_TXN_CURRENT.PAYMENT_ID.eq(update.paymentId()))
                .and(PAYMENT_TXN_CURRENT.DOMAIN_EVENT_ID.lt(update.domainEventId()))
                .execute();
        if (updated > 0) {
            return true;
        }
        if (!canInsert(update)) {
            return false;
        }
        var inserted = dslContext.insertInto(
                        PAYMENT_TXN_CURRENT,
                        PAYMENT_TXN_CURRENT.INVOICE_ID,
                        PAYMENT_TXN_CURRENT.PAYMENT_ID,
                        PAYMENT_TXN_CURRENT.DOMAIN_EVENT_ID,
                        PAYMENT_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT,
                        PAYMENT_TXN_CURRENT.PARTY_ID,
                        PAYMENT_TXN_CURRENT.SHOP_ID,
                        PAYMENT_TXN_CURRENT.SHOP_NAME,
                        PAYMENT_TXN_CURRENT.CREATED_AT,
                        PAYMENT_TXN_CURRENT.FINALIZED_AT,
                        PAYMENT_TXN_CURRENT.STATUS,
                        PAYMENT_TXN_CURRENT.PROVIDER_ID,
                        PAYMENT_TXN_CURRENT.PROVIDER_NAME,
                        PAYMENT_TXN_CURRENT.TERMINAL_ID,
                        PAYMENT_TXN_CURRENT.TERMINAL_NAME,
                        PAYMENT_TXN_CURRENT.AMOUNT,
                        PAYMENT_TXN_CURRENT.FEE,
                        PAYMENT_TXN_CURRENT.CURRENCY,
                        PAYMENT_TXN_CURRENT.TRX_ID,
                        PAYMENT_TXN_CURRENT.EXTERNAL_ID,
                        PAYMENT_TXN_CURRENT.RRN,
                        PAYMENT_TXN_CURRENT.APPROVAL_CODE,
                        PAYMENT_TXN_CURRENT.PAYMENT_TOOL_TYPE,
                        PAYMENT_TXN_CURRENT.ORIGINAL_AMOUNT,
                        PAYMENT_TXN_CURRENT.ORIGINAL_CURRENCY,
                        PAYMENT_TXN_CURRENT.CONVERTED_AMOUNT,
                        PAYMENT_TXN_CURRENT.EXCHANGE_RATE_INTERNAL,
                        PAYMENT_TXN_CURRENT.PROVIDER_AMOUNT,
                        PAYMENT_TXN_CURRENT.PROVIDER_CURRENCY,
                        PAYMENT_TXN_CURRENT.SHOP_SEARCH,
                        PAYMENT_TXN_CURRENT.PROVIDER_SEARCH,
                        PAYMENT_TXN_CURRENT.TERMINAL_SEARCH,
                        PAYMENT_TXN_CURRENT.TRX_SEARCH
                )
                .values(
                        update.invoiceId(),
                        update.paymentId(),
                        update.domainEventId(),
                        toLocalDateTime(update.domainEventCreatedAt()),
                        update.partyId(),
                        update.shopId(),
                        update.shopName(),
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
                        update.rrn(),
                        update.approvalCode(),
                        update.paymentToolType(),
                        update.originalAmount(),
                        update.originalCurrency(),
                        update.convertedAmount(),
                        update.exchangeRateInternal(),
                        update.providerAmount(),
                        update.providerCurrency(),
                        shopSearch(update),
                        providerSearch(update),
                        terminalSearch(update),
                        trxSearch(update)
                )
                .onConflict(PAYMENT_TXN_CURRENT.INVOICE_ID, PAYMENT_TXN_CURRENT.PAYMENT_ID)
                .doNothing()
                .execute();
        return inserted > 0;
    }

    private LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }

    private String shopSearch(PaymentCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.shopId(), update.shopName());
    }

    private String providerSearch(PaymentCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.providerId(), update.providerName());
    }

    private String terminalSearch(PaymentCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.terminalId(), update.terminalName());
    }

    private String trxSearch(PaymentCurrentUpdate update) {
        return SearchValueNormalizer.normalize(update.trxId(), update.rrn(), update.approvalCode());
    }

    private boolean canInsert(PaymentCurrentUpdate update) {
        return update.partyId() != null
                && update.createdAt() != null
                && update.status() != null
                && update.amount() != null
                && update.currency() != null;
    }

    private static <T> Field<T> patchValue(T value, TableField<?, T> field) {
        return DSL.coalesce(DSL.val(value, field.getDataType()), field);
    }

    private static <T> Field<T> firstWriteWins(T value, TableField<?, T> field) {
        return DSL.coalesce(field, DSL.val(value, field.getDataType()));
    }
}
