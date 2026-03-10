package dev.vality.ccreporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.port=0",
                "management.server.port=0"
        }
)
class ServiceApplicationTest {

    private static final EmbeddedPostgres EMBEDDED_POSTGRES = startPostgres();
    private static final String JDBC_URL = EMBEDDED_POSTGRES.getJdbcUrl("postgres", "postgres");

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @DynamicPropertySource
    static void registerDatasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> JDBC_URL);
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "");
    }

    @Test
    void applicationStartsWithDatabase() {
        Integer result = jdbcTemplate.queryForObject("select 1", Integer.class);
        Integer flywayHistoryTableCount = jdbcTemplate.queryForObject(
                "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
                Integer.class,
                "ccr",
                "flyway_schema_history"
        );
        Integer reportJobTableCount = jdbcTemplate.queryForObject(
                "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
                Integer.class,
                "ccr",
                "report_job"
        );

        assertThat(result).isEqualTo(1);
        assertThat(flywayHistoryTableCount).isEqualTo(1);
        assertThat(reportJobTableCount).isEqualTo(1);
    }

    @AfterAll
    static void stopPostgres() throws IOException {
        EMBEDDED_POSTGRES.close();
    }

    private static EmbeddedPostgres startPostgres() {
        try {
            return EmbeddedPostgres.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start embedded PostgreSQL for test", ex);
        }
    }
}
