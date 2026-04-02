package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Set;

import static dev.vality.ccreporter.dao.support.DaoUpsertUtils.*;
import static dev.vality.ccreporter.domain.Tables.PAYMENT_TXN_CURRENT;

@Repository
@RequiredArgsConstructor
public class PaymentTxnCurrentDao {

    private static final Set<Field<?>> IMMUTABLE_FIELDS = Set.of(
            PAYMENT_TXN_CURRENT.ID,
            PAYMENT_TXN_CURRENT.INVOICE_ID,
            PAYMENT_TXN_CURRENT.PAYMENT_ID,
            PAYMENT_TXN_CURRENT.UPDATED_AT
    );

    private static final Set<Field<?>> OVERWRITE_FIELDS = Set.of(
            PAYMENT_TXN_CURRENT.DOMAIN_EVENT_ID,
            PAYMENT_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT
    );

    private final DSLContext dslContext;

    public void upsert(PaymentTxnCurrent update) {
        var record = dslContext.newRecord(PAYMENT_TXN_CURRENT, update);
        record.changed(PAYMENT_TXN_CURRENT.ID, false);
        record.changed(PAYMENT_TXN_CURRENT.UPDATED_AT, false);

        dslContext.insertInto(PAYMENT_TXN_CURRENT)
                .set(record)
                .onConflict(PAYMENT_TXN_CURRENT.INVOICE_ID, PAYMENT_TXN_CURRENT.PAYMENT_ID)
                .doUpdate()
                .set(buildUpsertMap(
                        PAYMENT_TXN_CURRENT,
                        IMMUTABLE_FIELDS,
                        OVERWRITE_FIELDS,
                        Map.of(PAYMENT_TXN_CURRENT.UPDATED_AT, UTC_NOW)
                ))
                .where(isIncomingEventNewer(
                        PAYMENT_TXN_CURRENT.DOMAIN_EVENT_CREATED_AT,
                        PAYMENT_TXN_CURRENT.DOMAIN_EVENT_ID
                ))
                .execute();
    }
}
