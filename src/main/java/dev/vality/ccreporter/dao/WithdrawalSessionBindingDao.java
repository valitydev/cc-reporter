package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ingestion.WithdrawalSessionBindingUpdate;
import dev.vality.ccreporter.util.TimestampUtils;
import java.util.Optional;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class WithdrawalSessionBindingDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public WithdrawalSessionBindingDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean upsert(WithdrawalSessionBindingUpdate update) {
        int affected = jdbcTemplate.update(
                """
                INSERT INTO ccr.withdrawal_session_binding_current (
                    session_id, withdrawal_id, domain_event_id, domain_event_created_at
                ) VALUES (
                    :sessionId, :withdrawalId, :domainEventId, :domainEventCreatedAt
                )
                ON CONFLICT (session_id) DO UPDATE
                SET withdrawal_id = EXCLUDED.withdrawal_id,
                    domain_event_id = EXCLUDED.domain_event_id,
                    domain_event_created_at = EXCLUDED.domain_event_created_at,
                    updated_at = (now() AT TIME ZONE 'utc')
                WHERE ccr.withdrawal_session_binding_current.domain_event_id < EXCLUDED.domain_event_id
                """,
                new MapSqlParameterSource()
                        .addValue("sessionId", update.sessionId())
                        .addValue("withdrawalId", update.withdrawalId())
                        .addValue("domainEventId", update.domainEventId())
                        .addValue("domainEventCreatedAt", TimestampUtils.toLocalDateTime(update.domainEventCreatedAt()))
        );
        return affected > 0;
    }

    public Optional<String> findWithdrawalId(String sessionId) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    """
                    SELECT withdrawal_id
                    FROM ccr.withdrawal_session_binding_current
                    WHERE session_id = :sessionId
                    """,
                    new MapSqlParameterSource("sessionId", sessionId),
                    String.class
            ));
        } catch (EmptyResultDataAccessException ex) {
            return Optional.empty();
        }
    }
}
