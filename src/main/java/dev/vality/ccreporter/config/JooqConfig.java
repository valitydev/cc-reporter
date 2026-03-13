package dev.vality.ccreporter.config;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import javax.sql.DataSource;

@Configuration
public class JooqConfig {

    @Bean
    public org.jooq.Configuration jooqConfiguration(DataSource dataSource) {
        var configuration = new DefaultConfiguration();
        configuration.set(SQLDialect.POSTGRES);
        configuration.set(new DataSourceConnectionProvider(new TransactionAwareDataSourceProxy(dataSource)));
        return configuration;
    }

    @Bean
    public DSLContext dslContext(org.jooq.Configuration configuration) {
        return new DefaultDSLContext(configuration);
    }
}
