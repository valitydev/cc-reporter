package dev.vality.ccreporter.handler.support;

import org.slf4j.LoggerFactory;

import java.util.function.Function;
import java.util.function.Supplier;

public interface ThriftLoggingHandler {

    default <T> T handleRequest(
            String method,
            Supplier<String> requestSummary,
            ThrowingSupplier<T> handler,
            Function<T, String> responseSummary
    ) throws Exception {
        var log = LoggerFactory.getLogger(getClass());
        log.info("Handling thrift {}: {}", method, requestSummary.get());
        try {
            var response = handler.call();
            log.info("Handled thrift {}: {}", method, responseSummary.apply(response));
            return response;
        } catch (Exception ex) {
            log.warn("Failed thrift {}: {}, error={}", method, requestSummary.get(), ex.toString());
            throw ex;
        }
    }

    default void handleRequest(
            String method,
            Supplier<String> requestSummary,
            ThrowingRunnable handler
    ) throws Exception {
        var log = LoggerFactory.getLogger(getClass());
        log.info("Handling thrift {}: {}", method, requestSummary.get());
        try {
            handler.run();
            log.info("Handled thrift {}: void", method);
        } catch (Exception ex) {
            log.warn("Failed thrift {}: {}, error={}", method, requestSummary.get(), ex.toString());
            throw ex;
        }
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    interface ThrowingSupplier<T> {
        T call() throws Exception;
    }
}
