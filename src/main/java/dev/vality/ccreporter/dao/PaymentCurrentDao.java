package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ingestion.PaymentCurrentUpdate;
import dev.vality.ccreporter.ingestion.SearchValueNormalizer;
import dev.vality.ccreporter.util.TimestampUtils;
import java.time.Instant;
import java.time.LocalDateTime;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentCurrentDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public PaymentCurrentDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean upsert(PaymentCurrentUpdate update) {
        var params = params(update);
        int updated = jdbcTemplate.update(
                """
                UPDATE ccr.payment_txn_current
                SET domain_event_id = :domainEventId,
                    domain_event_created_at = :domainEventCreatedAt,
                    party_id = COALESCE(:partyId, party_id),
                    shop_id = COALESCE(:shopId, shop_id),
                    shop_name = COALESCE(:shopName, shop_name),
                    created_at = COALESCE(:createdAt, created_at),
                    finalized_at = COALESCE(finalized_at, :finalizedAt),
                    status = COALESCE(:status, status),
                    provider_id = COALESCE(:providerId, provider_id),
                    provider_name = COALESCE(:providerName, provider_name),
                    terminal_id = COALESCE(:terminalId, terminal_id),
                    terminal_name = COALESCE(:terminalName, terminal_name),
                    amount = COALESCE(:amount, amount),
                    fee = COALESCE(:fee, fee),
                    currency = COALESCE(:currency, currency),
                    trx_id = COALESCE(:trxId, trx_id),
                    external_id = COALESCE(:externalId, external_id),
                    rrn = COALESCE(:rrn, rrn),
                    approval_code = COALESCE(:approvalCode, approval_code),
                    payment_tool_type = COALESCE(:paymentToolType, payment_tool_type),
                    original_amount = COALESCE(:originalAmount, original_amount),
                    original_currency = COALESCE(:originalCurrency, original_currency),
                    converted_amount = COALESCE(:convertedAmount, converted_amount),
                    exchange_rate_internal = COALESCE(:exchangeRateInternal, exchange_rate_internal),
                    provider_amount = COALESCE(:providerAmount, provider_amount),
                    provider_currency = COALESCE(:providerCurrency, provider_currency),
                    shop_search = COALESCE(:shopSearch, shop_search),
                    provider_search = COALESCE(:providerSearch, provider_search),
                    terminal_search = COALESCE(:terminalSearch, terminal_search),
                    trx_search = COALESCE(:trxSearch, trx_search),
                    updated_at = (now() AT TIME ZONE 'utc')
                WHERE invoice_id = :invoiceId
                  AND payment_id = :paymentId
                  AND domain_event_id < :domainEventId
                """,
                params
        );
        if (updated > 0) {
            return true;
        }
        if (!canInsert(update)) {
            return false;
        }
        int inserted = jdbcTemplate.update(
                """
                INSERT INTO ccr.payment_txn_current (
                    invoice_id, payment_id, domain_event_id, domain_event_created_at, party_id, shop_id, shop_name,
                    created_at, finalized_at, status, provider_id, provider_name, terminal_id, terminal_name, amount,
                    fee, currency, trx_id, external_id, rrn, approval_code, payment_tool_type, original_amount,
                    original_currency, converted_amount, exchange_rate_internal, provider_amount, provider_currency,
                    shop_search, provider_search, terminal_search, trx_search
                ) VALUES (
                    :invoiceId, :paymentId, :domainEventId, :domainEventCreatedAt, :partyId, :shopId, :shopName,
                    :createdAt, :finalizedAt, :status, :providerId, :providerName, :terminalId, :terminalName, :amount,
                    :fee, :currency, :trxId, :externalId, :rrn, :approvalCode, :paymentToolType, :originalAmount,
                    :originalCurrency, :convertedAmount, :exchangeRateInternal, :providerAmount, :providerCurrency,
                    :shopSearch, :providerSearch, :terminalSearch, :trxSearch
                )
                ON CONFLICT (invoice_id, payment_id) DO NOTHING
                """,
                params
        );
        return inserted > 0;
    }

    private MapSqlParameterSource params(PaymentCurrentUpdate update) {
        return new MapSqlParameterSource()
                .addValue("invoiceId", update.invoiceId())
                .addValue("paymentId", update.paymentId())
                .addValue("domainEventId", update.domainEventId())
                .addValue("domainEventCreatedAt", toLocalDateTime(update.domainEventCreatedAt()))
                .addValue("partyId", update.partyId())
                .addValue("shopId", update.shopId())
                .addValue("shopName", update.shopName())
                .addValue("createdAt", toLocalDateTime(update.createdAt()))
                .addValue("finalizedAt", toLocalDateTime(update.finalizedAt()))
                .addValue("status", update.status())
                .addValue("providerId", update.providerId())
                .addValue("providerName", update.providerName())
                .addValue("terminalId", update.terminalId())
                .addValue("terminalName", update.terminalName())
                .addValue("amount", update.amount())
                .addValue("fee", update.fee())
                .addValue("currency", update.currency())
                .addValue("trxId", update.trxId())
                .addValue("externalId", update.externalId())
                .addValue("rrn", update.rrn())
                .addValue("approvalCode", update.approvalCode())
                .addValue("paymentToolType", update.paymentToolType())
                .addValue("originalAmount", update.originalAmount())
                .addValue("originalCurrency", update.originalCurrency())
                .addValue("convertedAmount", update.convertedAmount())
                .addValue("exchangeRateInternal", update.exchangeRateInternal())
                .addValue("providerAmount", update.providerAmount())
                .addValue("providerCurrency", update.providerCurrency())
                .addValue("shopSearch", SearchValueNormalizer.normalize(update.shopId(), update.shopName()))
                .addValue("providerSearch", SearchValueNormalizer.normalize(update.providerId(), update.providerName()))
                .addValue("terminalSearch", SearchValueNormalizer.normalize(update.terminalId(), update.terminalName()))
                .addValue("trxSearch", SearchValueNormalizer.normalize(update.trxId(), update.rrn(), update.approvalCode()));
    }

    private LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }

    private boolean canInsert(PaymentCurrentUpdate update) {
        return update.partyId() != null
                && update.createdAt() != null
                && update.status() != null
                && update.amount() != null
                && update.currency() != null;
    }
}
