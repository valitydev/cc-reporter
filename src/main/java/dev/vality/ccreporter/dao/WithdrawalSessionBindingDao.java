package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ingestion.WithdrawalSessionBindingUpdate;
import dev.vality.ccreporter.util.TimestampUtils;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.WITHDRAWAL_SESSION_BINDING_CURRENT;

@Repository
public class WithdrawalSessionBindingDao {

    private static final org.jooq.Field<LocalDateTime> UTC_NOW =
            DSL.field("(now() AT TIME ZONE 'utc')", LocalDateTime.class);

    private final DSLContext dslContext;

    public WithdrawalSessionBindingDao(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public boolean upsert(WithdrawalSessionBindingUpdate update) {
        var affected = dslContext.insertInto(
                        WITHDRAWAL_SESSION_BINDING_CURRENT,
                        WITHDRAWAL_SESSION_BINDING_CURRENT.SESSION_ID,
                        WITHDRAWAL_SESSION_BINDING_CURRENT.WITHDRAWAL_ID,
                        WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_ID,
                        WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_CREATED_AT
                )
                .values(
                        update.sessionId(),
                        update.withdrawalId(),
                        update.domainEventId(),
                        TimestampUtils.toLocalDateTime(update.domainEventCreatedAt())
                )
                .onConflict(WITHDRAWAL_SESSION_BINDING_CURRENT.SESSION_ID)
                .doUpdate()
                .set(
                        WITHDRAWAL_SESSION_BINDING_CURRENT.WITHDRAWAL_ID,
                        DSL.excluded(WITHDRAWAL_SESSION_BINDING_CURRENT.WITHDRAWAL_ID)
                )
                .set(
                        WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_ID,
                        DSL.excluded(WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_ID)
                )
                .set(
                        WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_CREATED_AT,
                        DSL.excluded(WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_CREATED_AT)
                )
                .set(WITHDRAWAL_SESSION_BINDING_CURRENT.UPDATED_AT, UTC_NOW)
                .where(
                        WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_ID.lt(
                                DSL.excluded(WITHDRAWAL_SESSION_BINDING_CURRENT.DOMAIN_EVENT_ID)
                        )
                )
                .execute();
        return affected > 0;
    }

    public Optional<String> findWithdrawalId(String sessionId) {
        return dslContext.select(WITHDRAWAL_SESSION_BINDING_CURRENT.WITHDRAWAL_ID)
                .from(WITHDRAWAL_SESSION_BINDING_CURRENT)
                .where(WITHDRAWAL_SESSION_BINDING_CURRENT.SESSION_ID.eq(sessionId))
                .fetchOptional(WITHDRAWAL_SESSION_BINDING_CURRENT.WITHDRAWAL_ID);
    }
}
