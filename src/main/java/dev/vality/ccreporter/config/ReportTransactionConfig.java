package dev.vality.ccreporter.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
public class ReportTransactionConfig {

    @Bean
    public ReportLifecycleTxTemplate reportLifecycleTxTemplate(PlatformTransactionManager transactionManager) {
        return new ReportLifecycleTxTemplate(transactionManager);
    }

    @Bean
    public ReportManagementTxTemplate reportManagementTxTemplate(PlatformTransactionManager transactionManager) {
        return new ReportManagementTxTemplate(transactionManager);
    }

    @Bean
    public ReportCsvReadOnlyTxTemplate reportCsvReadOnlyTxTemplate(PlatformTransactionManager transactionManager) {
        return new ReportCsvReadOnlyTxTemplate(transactionManager);
    }

    public static class ReportLifecycleTxTemplate extends TransactionTemplate {

        public ReportLifecycleTxTemplate(PlatformTransactionManager transactionManager) {
            super(transactionManager);
        }
    }

    public static class ReportManagementTxTemplate extends TransactionTemplate {

        public ReportManagementTxTemplate(PlatformTransactionManager transactionManager) {
            super(transactionManager);
        }
    }

    public static class ReportCsvReadOnlyTxTemplate extends TransactionTemplate {

        public ReportCsvReadOnlyTxTemplate(PlatformTransactionManager transactionManager) {
            super(transactionManager);
            setReadOnly(true);
            setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        }
    }
}
