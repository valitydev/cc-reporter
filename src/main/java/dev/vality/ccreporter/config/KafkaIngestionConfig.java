package dev.vality.ccreporter.config;

import dev.vality.ccreporter.config.properties.CcrKafkaProperties;
import dev.vality.ccreporter.serde.thrift.MachineEventParser;
import dev.vality.ccreporter.serde.thrift.ThriftDeserializer;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@RequiredArgsConstructor
@EnableConfigurationProperties(CcrKafkaProperties.class)
public class KafkaIngestionConfig {

    private final KafkaProperties kafkaProperties;
    private final CcrKafkaProperties ccrKafkaProperties;

    @Bean
    public MachineEventParser<EventPayload> paymentEventPayloadMachineEventParser() {
        return new MachineEventParser<>(new ThriftDeserializer<>(EventPayload::new));
    }

    @Bean
    public MachineEventParser<dev.vality.fistful.withdrawal.TimestampedChange> withdrawalEventMachineEventParser() {
        return new MachineEventParser<>(new ThriftDeserializer<>(dev.vality.fistful.withdrawal.TimestampedChange::new));
    }

    @Bean
    public MachineEventParser<dev.vality.fistful.withdrawal_session.TimestampedChange>
            withdrawalSessionEventMachineEventParser() {
        return new MachineEventParser<>(
                new ThriftDeserializer<>(dev.vality.fistful.withdrawal_session.TimestampedChange::new)
        );
    }

    @Bean
    public ConsumerFactory<String, SinkEvent> sinkEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                new ThriftDeserializer<>(SinkEvent::new)
        );
    }

    @Bean
    public ConsumerFactory<String, HistoricalCommit> dominantConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                new ThriftDeserializer<>(HistoricalCommit::new)
        );
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        var consumer = ccrKafkaProperties.getConsumer();
        return createErrorHandler(consumer.getErrorBackoffIntervalMs(), consumer.getErrorMaxAttempts());
    }

    @Bean
    public DefaultErrorHandler dominantKafkaErrorHandler() {
        var consumer = ccrKafkaProperties.getConsumer();
        return createErrorHandler(
                consumer.getDominantErrorBackoffIntervalMs(),
                consumer.getDominantErrorMaxAttempts()
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SinkEvent> paymentsKafkaListenerContainerFactory(
            ConsumerFactory<String, SinkEvent> sinkEventConsumerFactory,
            @Qualifier("kafkaErrorHandler") DefaultErrorHandler kafkaErrorHandler
    ) {
        return listenerContainerFactory(
                sinkEventConsumerFactory,
                ccrKafkaProperties.getConsumer().getPaymentsConcurrency(),
                kafkaErrorHandler
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SinkEvent> withdrawalsKafkaListenerContainerFactory(
            ConsumerFactory<String, SinkEvent> sinkEventConsumerFactory,
            @Qualifier("kafkaErrorHandler") DefaultErrorHandler kafkaErrorHandler
    ) {
        return listenerContainerFactory(
                sinkEventConsumerFactory,
                ccrKafkaProperties.getConsumer().getWithdrawalsConcurrency(),
                kafkaErrorHandler
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, HistoricalCommit> dominantKafkaListenerContainerFactory(
            ConsumerFactory<String, HistoricalCommit> dominantConsumerFactory,
            @Qualifier("dominantKafkaErrorHandler") DefaultErrorHandler dominantKafkaErrorHandler
    ) {
        return listenerContainerFactory(
                dominantConsumerFactory,
                ccrKafkaProperties.getConsumer().getDominantConcurrency(),
                dominantKafkaErrorHandler
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SinkEvent> withdrawalSessionsKafkaListenerContainerFactory(
            ConsumerFactory<String, SinkEvent> sinkEventConsumerFactory,
            @Qualifier("kafkaErrorHandler") DefaultErrorHandler kafkaErrorHandler
    ) {
        return listenerContainerFactory(
                sinkEventConsumerFactory,
                ccrKafkaProperties.getConsumer().getWithdrawalSessionsConcurrency(),
                kafkaErrorHandler
        );
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory,
            int concurrency,
            DefaultErrorHandler errorHandler
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(errorHandler);
        configureListener(factory);
        return factory;
    }

    private void configureListener(ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
        var listener = kafkaProperties.getListener();
        if (listener.getAckMode() != null) {
            factory.getContainerProperties().setAckMode(listener.getAckMode());
        }
        if (listener.getPollTimeout() != null) {
            factory.getContainerProperties().setPollTimeout(listener.getPollTimeout().toMillis());
        }
    }

    private DefaultErrorHandler createErrorHandler(long interval, long maxAttempts) {
        return new DefaultErrorHandler(new FixedBackOff(interval, resolveMaxAttempts(maxAttempts)));
    }

    private long resolveMaxAttempts(long maxAttempts) {
        return maxAttempts < 0 ? FixedBackOff.UNLIMITED_ATTEMPTS : maxAttempts;
    }
}
