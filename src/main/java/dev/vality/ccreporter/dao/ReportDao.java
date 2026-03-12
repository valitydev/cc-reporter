package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.report.StoredReport;
import dev.vality.ccreporter.storage.StoredFileData;
import dev.vality.ccreporter.util.ContinuationTokenCodec.PageCursor;
import dev.vality.ccreporter.util.ThriftQueryCodec;
import dev.vality.ccreporter.util.TimestampUtils;
import org.postgresql.util.PGobject;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Repository
public class ReportDao {

    private static final RowMapper<StoredReport> REPORT_ROW_MAPPER = ReportDao::mapReport;
    private static final RowMapper<StoredFileData> FILE_ROW_MAPPER = ReportDao::mapFile;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ThriftQueryCodec thriftQueryCodec;

    public ReportDao(NamedParameterJdbcTemplate jdbcTemplate, ThriftQueryCodec thriftQueryCodec) {
        this.jdbcTemplate = jdbcTemplate;
        this.thriftQueryCodec = thriftQueryCodec;
    }

    public Optional<Long> findByIdempotencyKey(String createdBy, String idempotencyKey) {
        if (!StringUtils.hasText(idempotencyKey)) {
            return Optional.empty();
        }
        try {
            var id = jdbcTemplate.queryForObject(
                    """
                            SELECT id
                            FROM ccr.report_job
                            WHERE created_by = :createdBy AND idempotency_key = :idempotencyKey
                            """,
                    new MapSqlParameterSource()
                            .addValue("createdBy", createdBy)
                            .addValue("idempotencyKey", idempotencyKey),
                    Long.class
            );
            return Optional.ofNullable(id);
        } catch (EmptyResultDataAccessException ex) {
            return Optional.empty();
        }
    }

    public long createReport(
            String createdBy,
            ReportType reportType,
            FileType fileType,
            ReportQuery query,
            String timezone,
            String idempotencyKey
    ) {
        var timeRange = thriftQueryCodec.extractTimeRange(query);
        var queryJson = thriftQueryCodec.serialize(query);
        var queryHash = thriftQueryCodec.hash(queryJson);

        return Objects.requireNonNull(
                jdbcTemplate.queryForObject(
                """
                        INSERT INTO ccr.report_job (
                            report_type,
                            file_type,
                            query_json,
                            query_hash,
                            requested_time_from,
                            requested_time_to,
                            timezone,
                            created_by,
                            idempotency_key
                        )
                        VALUES (
                            CAST(:reportType AS ccr.report_type),
                            CAST(:fileType AS ccr.file_type),
                            CAST(:queryJson AS jsonb),
                            :queryHash,
                            :requestedTimeFrom,
                            :requestedTimeTo,
                            :timezone,
                            :createdBy,
                            :idempotencyKey
                        )
                        RETURNING id
                        """,
                        new MapSqlParameterSource()
                        .addValue("reportType", reportType.name())
                        .addValue("fileType", fileType.name())
                        .addValue("queryJson", queryJson)
                        .addValue("queryHash", queryHash)
                        .addValue("requestedTimeFrom", TimestampUtils.toLocalDateTime(timeRange.from()))
                        .addValue("requestedTimeTo", TimestampUtils.toLocalDateTime(timeRange.to()))
                        .addValue("timezone", timezone)
                        .addValue("createdBy", createdBy)
                        .addValue("idempotencyKey", StringUtils.hasText(idempotencyKey) ? idempotencyKey : null),
                        Long.class
                ),
                "Report creation must return an id"
        );
    }

    public Optional<StoredReport> getReport(String createdBy, long reportId) {
        var result = jdbcTemplate.query(
                """
                        SELECT r.id,
                               r.report_type,
                               r.file_type,
                               r.query_json,
                               r.timezone,
                               r.status,
                               r.created_at,
                               r.updated_at,
                               r.started_at,
                               r.data_snapshot_fixed_at,
                               r.finished_at,
                               r.rows_count,
                               r.expires_at,
                               r.error_code,
                               r.error_message,
                               rf.file_id,
                               rf.file_type AS report_file_type,
                               rf.filename,
                               rf.content_type,
                               rf.md5,
                               rf.sha256,
                               rf.size_bytes,
                               rf.created_at AS file_created_at
                        FROM ccr.report_job r
                        LEFT JOIN ccr.report_file rf ON rf.report_id = r.id
                        WHERE r.id = :reportId AND r.created_by = :createdBy
                        """,
                new MapSqlParameterSource()
                        .addValue("reportId", reportId)
                        .addValue("createdBy", createdBy),
                REPORT_ROW_MAPPER
        );
        return result.stream().findFirst();
    }

    public List<StoredReport> getReports(
            String createdBy,
            GetReportsFilter filter,
            PageCursor cursor,
            int limit
    ) {
        var sql = new StringBuilder(
                """
                        SELECT r.id,
                               r.report_type,
                               r.file_type,
                               r.query_json,
                               r.timezone,
                               r.status,
                               r.created_at,
                               r.updated_at,
                               r.started_at,
                               r.data_snapshot_fixed_at,
                               r.finished_at,
                               r.rows_count,
                               r.expires_at,
                               r.error_code,
                               r.error_message,
                               rf.file_id,
                               rf.file_type AS report_file_type,
                               rf.filename,
                               rf.content_type,
                               rf.md5,
                               rf.sha256,
                               rf.size_bytes,
                               rf.created_at AS file_created_at
                        FROM ccr.report_job r
                        LEFT JOIN ccr.report_file rf ON rf.report_id = r.id
                        WHERE r.created_by = :createdBy
                        """
        );
        var parameters = new MapSqlParameterSource().addValue("createdBy", createdBy);

        if (filter != null && filter.isSetStatuses() && !filter.getStatuses().isEmpty()) {
            sql.append(" AND r.status::text IN (:statuses)");
            parameters.addValue("statuses", filter.getStatuses().stream().map(Enum::name).toList());
        }
        if (filter != null && filter.isSetReportTypes() && !filter.getReportTypes().isEmpty()) {
            sql.append(" AND r.report_type::text IN (:reportTypes)");
            parameters.addValue("reportTypes", filter.getReportTypes().stream().map(Enum::name).toList());
        }
        if (filter != null && filter.isSetFileTypes() && !filter.getFileTypes().isEmpty()) {
            sql.append(" AND r.file_type::text IN (:fileTypes)");
            parameters.addValue("fileTypes", filter.getFileTypes().stream().map(Enum::name).toList());
        }
        if (filter != null && filter.isSetCreatedFrom()) {
            sql.append(" AND r.created_at >= :createdFrom");
            parameters.addValue(
                    "createdFrom",
                    TimestampUtils.toLocalDateTime(TimestampUtils.parse(filter.getCreatedFrom()))
            );
        }
        if (filter != null && filter.isSetCreatedTo()) {
            sql.append(" AND r.created_at <= :createdTo");
            parameters.addValue(
                    "createdTo",
                    TimestampUtils.toLocalDateTime(TimestampUtils.parse(filter.getCreatedTo()))
            );
        }
        if (cursor != null) {
            sql.append(" AND (r.created_at < :cursorCreatedAt OR " +
                    "(r.created_at = :cursorCreatedAt AND r.id < :cursorId))");
            parameters.addValue("cursorCreatedAt", TimestampUtils.toLocalDateTime(cursor.createdAt()));
            parameters.addValue("cursorId", cursor.reportId());
        }

        sql.append(" ORDER BY r.created_at DESC, r.id DESC LIMIT :limit");
        parameters.addValue("limit", limit);
        return jdbcTemplate.query(sql.toString(), parameters, REPORT_ROW_MAPPER);
    }

    public boolean cancelReport(String createdBy, long reportId, Instant now) {
        var updated = jdbcTemplate.update(
                """
                        UPDATE ccr.report_job
                        SET status = CAST(:canceledStatus AS ccr.report_status),
                            finished_at = COALESCE(finished_at, :finishedAt),
                            updated_at = :updatedAt
                        WHERE id = :reportId
                          AND created_by = :createdBy
                          AND status::text IN (:cancelableStatuses)
                        """,
                new MapSqlParameterSource()
                        .addValue("canceledStatus", ReportStatus.canceled.name())
                        .addValue("finishedAt", TimestampUtils.toLocalDateTime(now))
                        .addValue("updatedAt", TimestampUtils.toLocalDateTime(now))
                        .addValue("reportId", reportId)
                        .addValue("createdBy", createdBy)
                        .addValue(
                                "cancelableStatuses",
                                List.of(ReportStatus.pending.name(), ReportStatus.processing.name())
                        )
        );
        return updated > 0;
    }

    public boolean reportExists(String createdBy, long reportId) {
        var count = jdbcTemplate.queryForObject(
                """
                        SELECT count(*)
                        FROM ccr.report_job
                        WHERE id = :reportId AND created_by = :createdBy
                        """,
                new MapSqlParameterSource()
                        .addValue("reportId", reportId)
                        .addValue("createdBy", createdBy),
                Integer.class
        );
        return count != null && count > 0;
    }

    public Optional<StoredFileData> getFile(String createdBy, String fileId) {
        var files = jdbcTemplate.query(
                """
                        SELECT rf.report_id,
                               rf.file_id,
                               rf.file_type,
                               rf.filename,
                               rf.content_type,
                               rf.md5,
                               rf.sha256,
                               rf.size_bytes,
                               rf.created_at,
                               rf.bucket,
                               rf.object_key
                        FROM ccr.report_file rf
                        JOIN ccr.report_job r ON r.id = rf.report_id
                        WHERE rf.file_id = :fileId AND r.created_by = :createdBy
                        """,
                new MapSqlParameterSource()
                        .addValue("fileId", fileId)
                        .addValue("createdBy", createdBy),
                FILE_ROW_MAPPER
        );
        return files.stream().findFirst();
    }

    private static StoredReport mapReport(ResultSet rs, int rowNum) throws SQLException {
        return new StoredReport(
                rs.getLong("id"),
                ReportType.valueOf(rs.getString("report_type")),
                FileType.valueOf(rs.getString("file_type")),
                readJson(rs.getObject("query_json")),
                rs.getString("timezone"),
                ReportStatus.valueOf(rs.getString("status")),
                TimestampUtils.toInstant(rs.getTimestamp("created_at")),
                TimestampUtils.toInstant(rs.getTimestamp("updated_at")),
                TimestampUtils.toOptionalInstant(rs.getTimestamp("started_at")),
                TimestampUtils.toOptionalInstant(rs.getTimestamp("data_snapshot_fixed_at")),
                TimestampUtils.toOptionalInstant(rs.getTimestamp("finished_at")),
                optionalLong(rs, "rows_count"),
                TimestampUtils.toOptionalInstant(rs.getTimestamp("expires_at")),
                rs.getString("error_code"),
                rs.getString("error_message"),
                rs.getString("file_id") == null ? null : new StoredFileData(
                        null,
                        rs.getString("file_id"),
                        FileType.valueOf(rs.getString("report_file_type")),
                        rs.getString("filename"),
                        rs.getString("content_type"),
                        rs.getString("md5"),
                        rs.getString("sha256"),
                        optionalLong(rs, "size_bytes"),
                        TimestampUtils.toInstant(rs.getTimestamp("file_created_at")),
                        null,
                        null
                )
        );
    }

    private static StoredFileData mapFile(ResultSet rs, int rowNum) throws SQLException {
        return new StoredFileData(
                rs.getLong("report_id"),
                rs.getString("file_id"),
                FileType.valueOf(rs.getString("file_type")),
                rs.getString("filename"),
                rs.getString("content_type"),
                rs.getString("md5"),
                rs.getString("sha256"),
                optionalLong(rs, "size_bytes"),
                TimestampUtils.toInstant(rs.getTimestamp("created_at")),
                rs.getString("bucket"),
                rs.getString("object_key")
        );
    }

    private static Long optionalLong(ResultSet rs, String columnName) throws SQLException {
        var value = rs.getLong(columnName);
        return rs.wasNull() ? null : value;
    }

    private static String readJson(Object value) throws SQLException {
        if (value instanceof PGobject pgObject) {
            return pgObject.getValue();
        }
        if (value instanceof String string) {
            return string;
        }
        throw new SQLException("Unsupported query_json type: " + value);
    }
}
