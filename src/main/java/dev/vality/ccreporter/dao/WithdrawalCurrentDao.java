package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ingestion.SearchValueNormalizer;
import dev.vality.ccreporter.ingestion.WithdrawalCurrentUpdate;
import dev.vality.ccreporter.util.TimestampUtils;
import java.time.Instant;
import java.time.LocalDateTime;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class WithdrawalCurrentDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public WithdrawalCurrentDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean upsert(WithdrawalCurrentUpdate update) {
        var params = params(update);
        if (isTransactionPatchOnly(update)) {
            int patched = jdbcTemplate.update(
                    """
                    UPDATE ccr.withdrawal_txn_current
                    SET trx_id = COALESCE(:trxId, trx_id),
                        trx_search = COALESCE(:trxSearch, trx_search),
                        updated_at = (now() AT TIME ZONE 'utc')
                    WHERE withdrawal_id = :withdrawalId
                      AND (:trxId IS NULL OR trx_id IS NULL OR trx_id = :trxId)
                    """,
                    params
            );
            if (patched > 0) {
                return true;
            }
        }
        int updated = jdbcTemplate.update(
                """
                UPDATE ccr.withdrawal_txn_current
                SET domain_event_id = :domainEventId,
                    domain_event_created_at = :domainEventCreatedAt,
                    party_id = COALESCE(:partyId, party_id),
                    wallet_id = COALESCE(:walletId, wallet_id),
                    wallet_name = COALESCE(:walletName, wallet_name),
                    destination_id = COALESCE(:destinationId, destination_id),
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
                    error_code = COALESCE(:errorCode, error_code),
                    error_reason = COALESCE(:errorReason, error_reason),
                    error_sub_failure = COALESCE(:errorSubFailure, error_sub_failure),
                    original_amount = COALESCE(:originalAmount, original_amount),
                    original_currency = COALESCE(:originalCurrency, original_currency),
                    converted_amount = COALESCE(:convertedAmount, converted_amount),
                    exchange_rate_internal = COALESCE(:exchangeRateInternal, exchange_rate_internal),
                    provider_amount = COALESCE(:providerAmount, provider_amount),
                    provider_currency = COALESCE(:providerCurrency, provider_currency),
                    wallet_search = COALESCE(:walletSearch, wallet_search),
                    provider_search = COALESCE(:providerSearch, provider_search),
                    terminal_search = COALESCE(:terminalSearch, terminal_search),
                    trx_search = COALESCE(:trxSearch, trx_search),
                    updated_at = (now() AT TIME ZONE 'utc')
                WHERE withdrawal_id = :withdrawalId
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
                INSERT INTO ccr.withdrawal_txn_current (
                    withdrawal_id, domain_event_id, domain_event_created_at, party_id, wallet_id, wallet_name,
                    destination_id, created_at, finalized_at, status, provider_id, provider_name, terminal_id,
                    terminal_name, amount, fee, currency, trx_id, external_id, error_code, error_reason,
                    error_sub_failure, original_amount, original_currency, converted_amount, exchange_rate_internal,
                    provider_amount, provider_currency, wallet_search, provider_search, terminal_search, trx_search
                ) VALUES (
                    :withdrawalId, :domainEventId, :domainEventCreatedAt, :partyId, :walletId, :walletName,
                    :destinationId, :createdAt, :finalizedAt, :status, :providerId, :providerName, :terminalId,
                    :terminalName, :amount, :fee, :currency, :trxId, :externalId, :errorCode, :errorReason,
                    :errorSubFailure, :originalAmount, :originalCurrency, :convertedAmount, :exchangeRateInternal,
                    :providerAmount, :providerCurrency, :walletSearch, :providerSearch, :terminalSearch, :trxSearch
                )
                ON CONFLICT (withdrawal_id) DO NOTHING
                """,
                params
        );
        return inserted > 0;
    }

    private MapSqlParameterSource params(WithdrawalCurrentUpdate update) {
        return new MapSqlParameterSource()
                .addValue("withdrawalId", update.withdrawalId())
                .addValue("domainEventId", update.domainEventId())
                .addValue("domainEventCreatedAt", toLocalDateTime(update.domainEventCreatedAt()))
                .addValue("partyId", update.partyId())
                .addValue("walletId", update.walletId())
                .addValue("walletName", update.walletName())
                .addValue("destinationId", update.destinationId())
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
                .addValue("errorCode", update.errorCode())
                .addValue("errorReason", update.errorReason())
                .addValue("errorSubFailure", update.errorSubFailure())
                .addValue("originalAmount", update.originalAmount())
                .addValue("originalCurrency", update.originalCurrency())
                .addValue("convertedAmount", update.convertedAmount())
                .addValue("exchangeRateInternal", update.exchangeRateInternal())
                .addValue("providerAmount", update.providerAmount())
                .addValue("providerCurrency", update.providerCurrency())
                .addValue("walletSearch", SearchValueNormalizer.normalize(update.walletId(), update.walletName()))
                .addValue("providerSearch", SearchValueNormalizer.normalize(update.providerId(), update.providerName()))
                .addValue("terminalSearch", SearchValueNormalizer.normalize(update.terminalId(), update.terminalName()))
                .addValue("trxSearch", SearchValueNormalizer.normalize(update.trxId()));
    }

    private LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
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
}
