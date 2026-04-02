package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalSession;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Set;

import static dev.vality.ccreporter.dao.support.DaoUpsertUtils.*;
import static dev.vality.ccreporter.domain.Tables.WITHDRAWAL_SESSION;

@Repository
@RequiredArgsConstructor
public class WithdrawalSessionDao {

    private static final Set<Field<?>> IMMUTABLE_FIELDS = Set.of(
            WITHDRAWAL_SESSION.SESSION_ID,
            WITHDRAWAL_SESSION.UPDATED_AT
    );

    private static final Set<Field<?>> OVERWRITE_FIELDS = Set.of(
            WITHDRAWAL_SESSION.DOMAIN_EVENT_ID,
            WITHDRAWAL_SESSION.DOMAIN_EVENT_CREATED_AT
    );

    private final DSLContext dslContext;

    public void upsert(WithdrawalSession update) {
        var record = dslContext.newRecord(WITHDRAWAL_SESSION, update);
        record.changed(WITHDRAWAL_SESSION.UPDATED_AT, false);

        dslContext.insertInto(WITHDRAWAL_SESSION)
                .set(record)
                .onConflict(WITHDRAWAL_SESSION.SESSION_ID)
                .doUpdate()
                .set(buildUpsertMap(
                        WITHDRAWAL_SESSION,
                        IMMUTABLE_FIELDS,
                        OVERWRITE_FIELDS,
                        Map.of(WITHDRAWAL_SESSION.UPDATED_AT, UTC_NOW)
                ))
                .where(isIncomingEventNewer(
                        WITHDRAWAL_SESSION.DOMAIN_EVENT_CREATED_AT,
                        WITHDRAWAL_SESSION.DOMAIN_EVENT_ID
                ))
                .execute();
    }
}
