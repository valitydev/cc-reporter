package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.PaymentsSearchFilter;
import dev.vality.ccreporter.WithdrawalsQuery;
import dev.vality.ccreporter.WithdrawalsSearchFilter;
import lombok.RequiredArgsConstructor;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static dev.vality.ccreporter.domain.Tables.*;
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

    public Cursor<? extends Record> fetchPayments(PaymentsQuery query) {
        var conditions = buildPaymentsConditions(query);
        return dslContext.select(
                        timestampField(PAYMENT_TXN_CURRENT.CREATED_AT, CREATED_AT),
                        timestampField(PAYMENT_TXN_CURRENT.FINALIZED_AT, FINALIZED_AT),
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
                        timestampField(WITHDRAWAL_TXN_CURRENT.CREATED_AT, CREATED_AT),
                        timestampField(WITHDRAWAL_TXN_CURRENT.FINALIZED_AT, FINALIZED_AT),
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
                        WITHDRAWAL_TXN_CURRENT.ORIGINAL_CURRENCY.as(ORIGINAL_CURRENCY)
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
        appendSearchCondition(conditions, lowercaseSearchField(SHOP_LOOKUP.SHOP_SEARCH), query.getFilter(), "shop");
        appendSearchCondition(
                conditions,
                lowercaseSearchField(PROVIDER_LOOKUP.PROVIDER_SEARCH),
                query.getFilter(),
                "provider"
        );
        appendSearchCondition(
                conditions,
                lowercaseSearchField(TERMINAL_LOOKUP.TERMINAL_SEARCH),
                query.getFilter(),
                "terminal"
        );
        appendSearchCondition(
                conditions,
                lowercaseSearchField(PAYMENT_TXN_CURRENT.TRX_SEARCH),
                query.getFilter(),
                "trx"
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
        appendSearchCondition(
                conditions,
                lowercaseSearchField(WALLET_LOOKUP.WALLET_SEARCH),
                query.getFilter(),
                "wallet"
        );
        appendSearchCondition(
                conditions,
                lowercaseSearchField(PROVIDER_LOOKUP.PROVIDER_SEARCH),
                query.getFilter(),
                "provider"
        );
        appendSearchCondition(
                conditions,
                lowercaseSearchField(TERMINAL_LOOKUP.TERMINAL_SEARCH),
                query.getFilter(),
                "terminal"
        );
        appendSearchCondition(conditions, lowercaseSearchField(latestSessionTrxSearch), query.getFilter(), "trx");
        return conditions;
    }

    private void appendInCondition(List<Condition> conditions, Field<String> field, List<String> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        conditions.add(field.in(values));
    }

    private void appendSearchCondition(
            List<Condition> conditions,
            Field<String> field,
            PaymentsSearchFilter filter,
            String filterType
    ) {
        if (filter == null) {
            return;
        }
        appendSearchCondition(conditions, field, switch (filterType) {
            case "shop" -> filter.getShopTerm();
            case "provider" -> filter.getProviderTerm();
            case "terminal" -> filter.getTerminalTerm();
            case "trx" -> filter.getTrxTerm();
            default -> null;
        });
    }

    private void appendSearchCondition(
            List<Condition> conditions,
            Field<String> field,
            WithdrawalsSearchFilter filter,
            String filterType
    ) {
        if (filter == null) {
            return;
        }
        appendSearchCondition(conditions, field, switch (filterType) {
            case "wallet" -> filter.getWalletTerm();
            case "provider" -> filter.getProviderTerm();
            case "terminal" -> filter.getTerminalTerm();
            case "trx" -> filter.getTrxTerm();
            default -> null;
        });
    }

    private void appendSearchCondition(List<Condition> conditions, Field<String> field, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        conditions.add(field.like("%" + value.toLowerCase() + "%"));
    }

    private Field<String> lowercaseSearchField(Field<String> field) {
        return DSL.lower(DSL.coalesce(field, ""));
    }

    private Field<java.sql.Timestamp> timestampField(Field<java.time.LocalDateTime> field, String alias) {
        return field.cast(SQLDataType.TIMESTAMP).as(alias);
    }
}
