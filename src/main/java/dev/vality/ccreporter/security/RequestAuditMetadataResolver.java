package dev.vality.ccreporter.security;

import dev.vality.ccreporter.model.RequestAuditMetadata;
import dev.vality.woody.api.trace.Metadata;
import dev.vality.woody.api.trace.TraceData;
import dev.vality.woody.api.trace.context.TraceContext;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityEmailExtensionKit;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityIdExtensionKit;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityRealmExtensionKit;
import dev.vality.woody.api.trace.context.metadata.user.UserIdentityUsernameExtensionKit;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Component
public class RequestAuditMetadataResolver {

    private static final String WOODY_USER_ID = UserIdentityIdExtensionKit.KEY;
    private static final String WOODY_USERNAME = UserIdentityUsernameExtensionKit.KEY;
    private static final String WOODY_EMAIL = UserIdentityEmailExtensionKit.KEY;
    private static final String WOODY_REALM = UserIdentityRealmExtensionKit.KEY;
    private static final String TRACE_PARENT = "traceparent";
    private static final String TRACE_STATE = "tracestate";

    public RequestAuditMetadata resolve() {
        var traceData = TraceContext.getCurrentTraceData();
        var activeSpan = traceData.getActiveSpan();
        var metadata = activeSpan.getCustomMetadata();
        var traceHeaders = extractTraceHeaders(traceData);
        return new RequestAuditMetadata(
                metadataValue(metadata, WOODY_USER_ID),
                metadataValue(metadata, WOODY_USERNAME),
                metadataValue(metadata, WOODY_EMAIL),
                metadataValue(metadata, WOODY_REALM),
                activeSpan.getSpan().getTraceId(),
                traceHeaders.get(TRACE_PARENT),
                traceHeaders.get(TRACE_STATE)
        );
    }

    private Map<String, String> extractTraceHeaders(TraceData traceData) {
        var headers = new HashMap<String, String>();
        var otelSpan = traceData.getOtelSpan();
        if (otelSpan != null && otelSpan.getSpanContext().isValid()) {
            GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(traceData.getOtelContext(), headers, MAP_SETTER);
        }
        putIfHasText(headers, TRACE_PARENT, traceData.getInboundTraceParent());
        putIfHasText(headers, TRACE_STATE, traceData.getInboundTraceState());
        return headers;
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

    private void putIfHasText(Map<String, String> headers, String key, String value) {
        if (StringUtils.hasText(value) && !headers.containsKey(key)) {
            headers.put(key, value.trim());
        }
    }

    private static final TextMapSetter<Map<String, String>> MAP_SETTER = (carrier, key, value) -> {
        if (carrier != null && StringUtils.hasText(key) && StringUtils.hasText(value)) {
            carrier.put(key, value);
        }
    };

}
