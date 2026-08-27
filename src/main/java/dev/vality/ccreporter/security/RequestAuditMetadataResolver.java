package dev.vality.ccreporter.security;

import dev.vality.ccreporter.model.RequestAuditMetadata;
import dev.vality.woody.api.trace.Metadata;
import dev.vality.woody.api.trace.context.TraceContext;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityEmailExtensionKit;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityIdExtensionKit;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityRealmExtensionKit;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityUsernameExtensionKit;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.stream.Collectors;

@Component
public class RequestAuditMetadataResolver {

    private static final String WOODY_USER_ID = UserIdentityIdExtensionKit.KEY;
    private static final String WOODY_USERNAME = UserIdentityUsernameExtensionKit.KEY;
    private static final String WOODY_EMAIL = UserIdentityEmailExtensionKit.KEY;
    private static final String WOODY_REALM = UserIdentityRealmExtensionKit.KEY;

    public RequestAuditMetadata resolve() {
        var traceData = TraceContext.getCurrentTraceData();
        var activeSpan = traceData.getActiveSpan();
        var metadata = activeSpan.getCustomMetadata();
        var spanContext = Span.current().getSpanContext();
        return new RequestAuditMetadata(
                metadataValue(metadata, WOODY_USER_ID),
                metadataValue(metadata, WOODY_USERNAME),
                metadataValue(metadata, WOODY_EMAIL),
                metadataValue(metadata, WOODY_REALM),
                resolveTraceId(spanContext, activeSpan.getSpan().getTraceId()),
                resolveTraceparent(spanContext),
                resolveTracestate(spanContext)
        );
    }

    private String resolveTraceId(SpanContext spanContext, String woodyTraceId) {
        return spanContext.isValid() ? spanContext.getTraceId() : woodyTraceId;
    }

    private String resolveTraceparent(SpanContext spanContext) {
        if (!spanContext.isValid()) {
            return null;
        }
        return "00-%s-%s-%s".formatted(
                spanContext.getTraceId(),
                spanContext.getSpanId(),
                spanContext.getTraceFlags().asHex()
        );
    }

    private String resolveTracestate(SpanContext spanContext) {
        if (!spanContext.isValid() || spanContext.getTraceState().isEmpty()) {
            return null;
        }
        return spanContext.getTraceState().asMap().entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(","));
    }

    private String metadataValue(Metadata metadata, String key) {
        if (metadata == null) {
            return null;
        }
        return normalize(metadata.getValue(key));
    }

    private String normalize(Object value) {
        if (value == null) {
            return null;
        }
        var stringValue = value.toString();
        if (StringUtils.hasText(stringValue)) {
            return stringValue.trim();
        }
        return null;
    }

}
