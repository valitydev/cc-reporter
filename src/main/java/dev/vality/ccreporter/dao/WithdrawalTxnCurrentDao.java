package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Set;

import static dev.vality.ccreporter.dao.support.DaoUpsertUtils.*;
import static dev.vality.ccreporter.domain.Tables.WITHDRAWAL_TXN_CURRENT;

@Repository
@RequiredArgsConstructor
public class WithdrawalTxnCurrentDao {

    private static final Set<Field<?>> IMMUTABLE_FIELDS = Set.of(
            WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID,
            WITHDRAWAL_TXN_CURRENT.UPDATED_AT
    );

    private static final Set<Field<?>> OVERWRITE_FIELDS = Set.of(
            WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_ID,
            WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT
    );

    private final DSLContext dslContext;

    public void upsert(WithdrawalTxnCurrent update) {
        var record = dslContext.newRecord(WITHDRAWAL_TXN_CURRENT, update);
        record.changed(WITHDRAWAL_TXN_CURRENT.UPDATED_AT, false);

        dslContext.insertInto(WITHDRAWAL_TXN_CURRENT)
                .set(record)
                .onConflict(WITHDRAWAL_TXN_CURRENT.WITHDRAWAL_ID)
                .doUpdate()
                .set(buildUpsertMap(
                        WITHDRAWAL_TXN_CURRENT,
                        IMMUTABLE_FIELDS,
                        OVERWRITE_FIELDS,
                        Map.of(WITHDRAWAL_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                ))
                .where(isIncomingEventNewer(
                        WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT,
                        WITHDRAWAL_TXN_CURRENT.DOMAIN_EVENT_ID
                ))
                .execute();
    }
}