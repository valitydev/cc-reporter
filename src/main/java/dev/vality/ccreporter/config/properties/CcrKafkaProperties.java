package dev.vality.ccreporter.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "ccr.kafka")
public class CcrKafkaProperties {

    private Consumer consumer;
    private Topics topics;

    @Data
    public static class Consumer {
        private int dominantConcurrency;
        private int paymentsConcurrency;
        private int withdrawalsConcurrency;
        private int withdrawalSessionsConcurrency;
        private long errorBackoffIntervalMs;
        private long errorMaxAttempts;
        private long dominantErrorBackoffIntervalMs;
        private long dominantErrorMaxAttempts;
    }

    @Data
    public static class Topics {
        private Topic dominant;
        private Topic payments;
        private Topic withdrawals;
        private Topic withdrawalSessions;
    }

    @Data
    public static class Topic {
        private String id;
        private boolean enabled;
    }
}
