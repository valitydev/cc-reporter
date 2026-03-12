package dev.vality.ccreporter.config;

import dev.vality.ccreporter.config.properties.CcrKafkaProperties;
import dev.vality.ccreporter.kafka.serde.SinkEventDeserializer;
import dev.vality.ccreporter.serialization.MachineEventPayloadParser;
import dev.vality.ccreporter.serialization.ThriftBinaryDeserializer;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.machinegun.eventsink.SinkEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
@EnableKafka
@EnableConfigurationProperties(CcrKafkaProperties.class)
public class KafkaIngestionConfig {

    @Bean
    public ThriftBinaryDeserializer<EventPayload> paymentEventPayloadDeserializer() {
        return new ThriftBinaryDeserializer<>() {
            @Override
            protected EventPayload newInstance() {
                return new EventPayload();
            }
        };
    }

    @Bean
    public MachineEventPayloadParser<EventPayload> paymentEventPayloadMachineEventParser(
            ThriftBinaryDeserializer<EventPayload> paymentEventPayloadDeserializer
    ) {
        return new MachineEventPayloadParser<>(paymentEventPayloadDeserializer);
    }

    @Bean
    public ThriftBinaryDeserializer<Event> withdrawalEventDeserializer() {
        return new ThriftBinaryDeserializer<>() {
            @Override
            protected Event newInstance() {
                return new Event();
            }
        };
    }

    @Bean
    public MachineEventPayloadParser<Event> withdrawalEventMachineEventParser(
            ThriftBinaryDeserializer<Event> withdrawalEventDeserializer) {
        return new MachineEventPayloadParser<>(withdrawalEventDeserializer);
    }

    @Bean
    public ThriftBinaryDeserializer<dev.vality.fistful.withdrawal_session.Event>
            withdrawalSessionEventDeserializer() {
        return new ThriftBinaryDeserializer<>() {
            @Override
            protected dev.vality.fistful.withdrawal_session.Event newInstance() {
                return new dev.vality.fistful.withdrawal_session.Event();
            }
        };
    }

    @Bean
    public MachineEventPayloadParser<dev.vality.fistful.withdrawal_session.Event>
            withdrawalSessionEventMachineEventParser(
            ThriftBinaryDeserializer<dev.vality.fistful.withdrawal_session.Event>
                    withdrawalSessionEventDeserializer
    ) {
        return new MachineEventPayloadParser<>(withdrawalSessionEventDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SinkEvent> paymentsKafkaListenerContainerFactory(
            Environment environment,
            CcrKafkaProperties ccrKafkaProperties
    ) {
        return listenerContainerFactory(
                environment,
                ccrKafkaProperties.getConsumer().getPaymentsConcurrency(),
                ccrKafkaProperties
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SinkEvent> withdrawalsKafkaListenerContainerFactory(
            Environment environment,
            CcrKafkaProperties ccrKafkaProperties
    ) {
        return listenerContainerFactory(
                environment,
                ccrKafkaProperties.getConsumer().getWithdrawalsConcurrency(),
                ccrKafkaProperties
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SinkEvent> withdrawalSessionsKafkaListenerContainerFactory(
            Environment environment,
            CcrKafkaProperties ccrKafkaProperties
    ) {
        return listenerContainerFactory(
                environment,
                ccrKafkaProperties.getConsumer().getWithdrawalsConcurrency(),
                ccrKafkaProperties
        );
    }

    private ConcurrentKafkaListenerContainerFactory<String, SinkEvent> listenerContainerFactory(
            Environment environment,
            int concurrency,
            CcrKafkaProperties ccrKafkaProperties
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, SinkEvent>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerConfig(environment)));
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(kafkaErrorHandler(ccrKafkaProperties));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    private DefaultErrorHandler kafkaErrorHandler(CcrKafkaProperties ccrKafkaProperties) {
        var maxAttempts = ccrKafkaProperties.getConsumer().getErrorMaxAttempts();
        return new DefaultErrorHandler(new FixedBackOff(
                ccrKafkaProperties.getConsumer().getErrorBackoffIntervalMs(),
                maxAttempts < 0 ? FixedBackOff.UNLIMITED_ATTEMPTS : maxAttempts
        ));
    }

    private Map<String, Object> consumerConfig(Environment environment) {
        var config = new java.util.HashMap<String, Object>();
        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("spring.kafka.bootstrap-servers", "localhost:9092")
        );
        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty("spring.kafka.consumer.group-id", "cc-reporter")
        );
        config.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                Boolean.parseBoolean(environment.getProperty("spring.kafka.consumer.enable-auto-commit", "false"))
        );
        config.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                environment.getProperty("spring.kafka.consumer.auto-offset-reset", "earliest")
        );
        config.put(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                Integer.parseInt(environment.getProperty("spring.kafka.consumer.max-poll-records", "20"))
        );
        config.put(
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                Integer.parseInt(
                        environment.getProperty("spring.kafka.consumer.properties.max.poll.interval.ms", "30000")
                )
        );
        config.put(
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                Integer.parseInt(
                        environment.getProperty("spring.kafka.consumer.properties.session.timeout.ms", "30000"))
        );
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SinkEventDeserializer.class);
        return config;
    }
}
