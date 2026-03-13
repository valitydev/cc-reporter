package dev.vality.ccreporter.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ccr.kafka")
public class CcrKafkaProperties {

    private Consumer consumer = new Consumer();
    private Topics topics = new Topics();

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public Topics getTopics() {
        return topics;
    }

    public void setTopics(Topics topics) {
        this.topics = topics;
    }

    public static class Consumer {
        private int dominantConcurrency = 1;
        private int paymentsConcurrency = 1;
        private int withdrawalsConcurrency = 1;
        private long errorBackoffIntervalMs = 30000L;
        private long errorMaxAttempts = -1L;

        public int getDominantConcurrency() {
            return dominantConcurrency;
        }

        public void setDominantConcurrency(int dominantConcurrency) {
            this.dominantConcurrency = dominantConcurrency;
        }

        public int getPaymentsConcurrency() {
            return paymentsConcurrency;
        }

        public void setPaymentsConcurrency(int paymentsConcurrency) {
            this.paymentsConcurrency = paymentsConcurrency;
        }

        public int getWithdrawalsConcurrency() {
            return withdrawalsConcurrency;
        }

        public void setWithdrawalsConcurrency(int withdrawalsConcurrency) {
            this.withdrawalsConcurrency = withdrawalsConcurrency;
        }

        public long getErrorBackoffIntervalMs() {
            return errorBackoffIntervalMs;
        }

        public void setErrorBackoffIntervalMs(long errorBackoffIntervalMs) {
            this.errorBackoffIntervalMs = errorBackoffIntervalMs;
        }

        public long getErrorMaxAttempts() {
            return errorMaxAttempts;
        }

        public void setErrorMaxAttempts(long errorMaxAttempts) {
            this.errorMaxAttempts = errorMaxAttempts;
        }
    }

    public static class Topics {
        private Topic dominant = new Topic();
        private Topic payments = new Topic();
        private Topic withdrawals = new Topic();
        private Topic withdrawalSessions = new Topic();

        public Topic getDominant() {
            return dominant;
        }

        public void setDominant(Topic dominant) {
            this.dominant = dominant;
        }

        public Topic getPayments() {
            return payments;
        }

        public void setPayments(Topic payments) {
            this.payments = payments;
        }

        public Topic getWithdrawals() {
            return withdrawals;
        }

        public void setWithdrawals(Topic withdrawals) {
            this.withdrawals = withdrawals;
        }

        public Topic getWithdrawalSessions() {
            return withdrawalSessions;
        }

        public void setWithdrawalSessions(Topic withdrawalSessions) {
            this.withdrawalSessions = withdrawalSessions;
        }
    }

    public static class Topic {
        private String id = "";
        private boolean enabled;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
