package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.WithdrawalsQuery;
import lombok.RequiredArgsConstructor;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static dev.vality.ccreporter.domain.Tables.*;
import static dev.vality.ccreporter.util.SearchValueNormalizer.normalize;
import static dev.vality.ccreporter.util.TimestampUtils.parse;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;

@Repository
@RequiredArgsConstructor
public class ReportCsvDao {

    private static final int CSV_QUERY_FETCH_SIZE = 1_000;
    private static final String CREATED_AT = "created_at";
    private static final String FINALIZED_AT = "finalized_at";
    private static final String PROVIDER_CURRENCY = "provider_currency";
    private static final String ORIGINAL_CURRENCY = "original_currency";
    private static final String CURRENCY = "currency";

    private final DSLContext dslContext;

    public Instant currentSnapshot() {
        return dslContext.select(DSL.currentOffsetDateTime())
                .fetchSingle(0, OffsetDateTime.class)
                .toInstant();
    }

    public void setLocalStatementTimeout(long timeoutMs) {
        dslContext.fetchSingle(
                "SELECT set_config('statement_timeout', ?, true)",
                timeoutMs + "ms"
        );
    }

    public Cursor<? extends Record> fetchPayments(PaymentsQuery query) {
        var conditions = buildPaymentsConditions(query);
        return dslContext.select(
                        PAYMENT_TXN_CURRENT.CREATED_AT.as(CREATED_AT),
                        PAYMENT_TXN_CURRENT.FINALIZED_AT.as(FINALIZED_AT),
                        PAYMENT_TXN_CURRENT.INVOICE_ID.as("invoice_id"),
                        PAYMENT_TXN_CURRENT.PAYMENT_ID.as("payment_id"),
                        PAYMENT_TXN_CURRENT.STATUS.as("status"),
                        PAYMENT_TXN_CURRENT.AMOUNT.as("amount"),
                        PAYMENT_TXN_CURRENT.CURRENCY.as(CURRENCY),
                        PAYMENT_TXN_CURRENT.TRX_ID.as("trx_id"),
                        PAYMENT_TXN_CURRENT.PROVIDER_ID.as("provider_id"),
                        PAYMENT_TXN_CURRENT.TERMINAL_ID.as("terminal_id"),
                        PAYMENT_TXN_CURRENT.SHOP_ID.as("shop_id"),
                        PAYMENT_TXN_CURRENT.EXCHANGE_RATE_INTERNAL.as("exchange_rate_internal"),
                        PAYMENT_TXN_CURRENT.PROVIDER_AMOUNT.as("provider_amount"),
                        PAYMENT_TXN_CURRENT.PROVIDER_CURRENCY.as(PROVIDER_CURRENCY),
                        PAYMENT_TXN_CURRENT.ORIGINAL_AMOUNT.as("original_amount"),
                        PAYMENT_TXN_CURRENT.ORIGINAL_CURRENCY.as(ORIGINAL_CURRENCY),
                        PAYMENT_TXN_CURRENT.CONVERTED_AMOUNT.as("converted_amount")
                )
                .from(PAYMENT_TXN_CURRENT)
                .leftJoin(SHOP_LOOKUP).on(SHOP_LOOKUP.SHOP_ID.eq(PAYMENT_TXN_CURRENT.SHOP_ID))
                .leftJoin(PROVIDER_LOOKUP).on(PROVIDER_LOOKUP.PROVIDER_ID.eq(PAYMENT_TXN_CURRENT.PROVIDER_ID))
                .leftJoin(TERMINAL_LOOKUP).on(TERMINAL_LOOKUP.TERMINAL_ID.eq(PAYMENT_TXN_CURRENT.TERMINAL_ID))
                .where(conditions)
                .orderBy(PAYMENT_TXN_CURRENT.CREATED_AT.asc(),
                        PAYMENT_TXN_CURRENT.INVOICE_ID.asc(),
                        PAYMENT_TXN_CURRENT.PAYMENT_ID.asc())
                .fetchSize(CSV_QUERY_FETCH_SIZE)
                .fetchLazy();
    }

    public Cursor<? extends Record> fetchWithdrawals(WithdrawalsQuery query) {
        var latestSessionQuery = DSL.select(
                        WITHDRAWAL_SESSION.SESSION_ID.as("session_id"),
                        WITHDRAWAL_SESSION.TRX_ID.as("trx_id"),
                        WITHDRAWAL_SESSION.TRX_SEARCH.as("trx_search")
                )
                .from(WITHDRAWAL_SESSION)
                .where(WITHDRAWAL_SESSION.WITHDRAWAL_ID.eq(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID))
                .orderBy(
                        WITHDRAWAL_SESSION.DOMAIN_EVENT_CREATED_AT.desc(),
                        WITHDRAWAL_SESSION.DOMAIN_EVENT_ID.desc(),
                        WITHDRAWAL_SESSION.SESSION_ID.desc()
                )
                .limit(1);
        var latestSession = DSL.lateral(latestSessionQuery).as("ws");
        var latestSessionTrxId = latestSession.field("trx_id", String.class);
        var latestSessionTrxSearch = latestSession.field("trx_search", String.class);
        var latestSessionSessionId = latestSession.field("session_id", String.class);
        var conditions = buildWithdrawalConditions(query, latestSessionTrxId, latestSessionTrxSearch);
        return dslContext.select(
                        WITHDRAWAL_TXN_CURRENT.CREATED_AT.as(CREATED_AT),
                        WITHDRAWAL_TXN_CURRENT.FINALIZED_AT.as(FINALIZED_AT),
                        WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID.as("withdrawal_id"),
                        WITHDRAWAL_TXN_CURRENT.STATUS.as("status"),
                        WITHDRAWAL_TXN_CURRENT.AMOUNT.as("amount"),
                        WITHDRAWAL_TXN_CURRENT.CURRENCY.as(CURRENCY),
                        latestSessionTrxId.as("trx_id"),
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_ID.as("provider_id"),
                        WITHDRAWAL_TXN_CURRENT.TERMINAL_ID.as("terminal_id"),
                        WITHDRAWAL_TXN_CURRENT.WALLET_ID.as("wallet_id"),
                        WITHDRAWAL_TXN_CURRENT.EXCHANGE_RATE_INTERNAL.as("exchange_rate_internal"),
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_AMOUNT.as("provider_amount"),
                        WITHDRAWAL_TXN_CURRENT.PROVIDER_CURRENCY.as(PROVIDER_CURRENCY),
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_AMOUNT.as("original_amount"),
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_CURRENCY.as(ORIGINAL_CURRENCY),
                        WITHDRAWAL_TXN_CURRENT.CONVERTED_AMOUNT.as("converted_amount")
                )
                .from(WITHDRAWAL_TXN_CURRENT)
                .leftJoin(latestSession).on(DSL.trueCondition())
                .leftJoin(WALLET_LOOKUP).on(WALLET_LOOKUP.WALLET_ID.eq(WITHDRAWAL_TXN_CURRENT.WALLET_ID))
                .leftJoin(PROVIDER_LOOKUP).on(PROVIDER_LOOKUP.PROVIDER_ID.eq(WITHDRAWAL_TXN_CURRENT.PROVIDER_ID))
                .leftJoin(TERMINAL_LOOKUP).on(TERMINAL_LOOKUP.TERMINAL_ID.eq(WITHDRAWAL_TXN_CURRENT.TERMINAL_ID))
                .where(conditions)
                .orderBy(
                        WITHDRAWAL_TXN_CURRENT.CREATED_AT.asc(),
                        WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID.asc(),
                        latestSessionSessionId.asc()
                )
                .fetchSize(CSV_QUERY_FETCH_SIZE)
                .fetchLazy();
    }

    private List<Condition> buildPaymentsConditions(PaymentsQuery query) {
        var conditions = new ArrayList<Condition>();
        conditions.add(PAYMENT_TXN_CURRENT.CREATED_AT.ge(toLocalDateTime(parse(query.getTimeRange().getFromTime()))));
        conditions.add(PAYMENT_TXN_CURRENT.CREATED_AT.lt(toLocalDateTime(parse(query.getTimeRange().getToTime()))));
        appendInCondition(conditions, PAYMENT_TXN_CURRENT.PARTY_ID, query.getPartyIds());
        appendInCondition(conditions, PAYMENT_TXN_CURRENT.SHOP_ID, query.getShopIds());
        appendInCondition(conditions, PAYMENT_TXN_CURRENT.PROVIDER_ID, query.getProviderIds());
        appendInCondition(conditions, PAYMENT_TXN_CURRENT.TERMINAL_ID, query.getTerminalIds());
        appendInCondition(conditions, PAYMENT_TXN_CURRENT.TRX_ID, query.getTrxIds());
        appendInCondition(conditions, PAYMENT_TXN_CURRENT.CURRENCY, query.getCurrencies());
        appendInCondition(conditions, PAYMENT_TXN_CURRENT.STATUS, query.getStatuses());
        var filter = query.getFilter();
        appendSearchCondition(
                conditions,
                SHOP_LOOKUP.SHOP_SEARCH,
                filter == null ? null : filter.getShopTerm()
        );
        appendSearchCondition(
                conditions,
                PROVIDER_LOOKUP.PROVIDER_SEARCH,
                filter == null ? null : filter.getProviderTerm()
        );
        appendSearchCondition(
                conditions,
                TERMINAL_LOOKUP.TERMINAL_SEARCH,
                filter == null ? null : filter.getTerminalTerm()
        );
        appendSearchCondition(
                conditions,
                PAYMENT_TXN_CURRENT.TRX_SEARCH,
                filter == null ? null : filter.getTrxTerm()
        );
        return conditions;
    }

    private List<Condition> buildWithdrawalConditions(
            WithdrawalsQuery query,
            Field<String> latestSessionTrxId,
            Field<String> latestSessionTrxSearch
    ) {
        var conditions = new ArrayList<Condition>();
        conditions.add(
                WITHDRAWAL_TXN_CURRENT.CREATED_AT.ge(toLocalDateTime(parse(query.getTimeRange().getFromTime())))
        );
        conditions.add(
                WITHDRAWAL_TXN_CURRENT.CREATED_AT.lt(toLocalDateTime(parse(query.getTimeRange().getToTime())))
        );
        appendInCondition(conditions, WITHDRAWAL_TXN_CURRENT.PARTY_ID, query.getPartyIds());
        appendInCondition(conditions, WITHDRAWAL_TXN_CURRENT.WALLET_ID, query.getWalletIds());
        appendInCondition(conditions, WITHDRAWAL_TXN_CURRENT.PROVIDER_ID, query.getProviderIds());
        appendInCondition(conditions, WITHDRAWAL_TXN_CURRENT.TERMINAL_ID, query.getTerminalIds());
        appendInCondition(conditions, latestSessionTrxId, query.getTrxIds());
        appendInCondition(conditions, WITHDRAWAL_TXN_CURRENT.CURRENCY, query.getCurrencies());
        appendInCondition(conditions, WITHDRAWAL_TXN_CURRENT.STATUS, query.getStatuses());
        var filter = query.getFilter();
        appendSearchCondition(
                conditions,
                WALLET_LOOKUP.WALLET_SEARCH,
                filter == null ? null : filter.getWalletTerm()
        );
        appendSearchCondition(
                conditions,
                PROVIDER_LOOKUP.PROVIDER_SEARCH,
                filter == null ? null : filter.getProviderTerm()
        );
        appendSearchCondition(
                conditions,
                TERMINAL_LOOKUP.TERMINAL_SEARCH,
                filter == null ? null : filter.getTerminalTerm()
        );
        appendSearchCondition(
                conditions,
                latestSessionTrxSearch,
                filter == null ? null : filter.getTrxTerm()
        );
        return conditions;
    }

    private void appendInCondition(List<Condition> conditions, Field<String> field, List<String> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        conditions.add(field.in(values));
    }

    private void appendSearchCondition(List<Condition> conditions, Field<String> field, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        var normalizedValue = normalize(value);
        var pattern = "%" + escapeLikeLiteral(normalizedValue) + "%";
        conditions.add(DSL.condition("{0} LIKE {1} ESCAPE '!'", field, DSL.val(pattern)));
    }

    private String escapeLikeLiteral(String value) {
        return value
                .replace("!", "!!")
                .replace("%", "!%")
                .replace("_", "!_");
    }
}
