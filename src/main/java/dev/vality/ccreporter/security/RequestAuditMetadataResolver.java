package dev.vality.ccreporter.security;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class RequestAuditMetadataResolver {

    private static final String WOODY_USER_ID = "woody.meta.user-identity.id";
    private static final String WOODY_USERNAME = "woody.meta.user-identity.username";
    private static final String WOODY_EMAIL = "woody.meta.user-identity.email";
    private static final String WOODY_REALM = "woody.meta.user-identity.realm";
    private static final String WOODY_REQUEST_ID = "woody.meta.user-identity.X-Request-ID";
    private static final String WOODY_REQUEST_DEADLINE = "woody.meta.user-identity.X-Request-Deadline";
    private static final String LEGACY_USER_ID = "x-woody-meta-user-identity-id";
    private static final String LEGACY_USERNAME = "x-woody-meta-user-identity-username";
    private static final String LEGACY_EMAIL = "x-woody-meta-user-identity-email";
    private static final String LEGACY_REALM = "x-woody-meta-user-identity-realm";
    private static final String LEGACY_REQUEST_ID = "x-woody-meta-user-identity-X-Request-ID";
    private static final String LEGACY_REQUEST_DEADLINE = "x-woody-meta-user-identity-X-Request-Deadline";
    private static final String REQUEST_ID = "X-Request-ID";
    private static final String REQUEST_DEADLINE = "X-Request-Deadline";
    private static final String TRACE_PARENT = "traceparent";
    private static final String TRACE_STATE = "tracestate";

    private final ObjectProvider<HttpServletRequest> requestProvider;

    public RequestAuditMetadataResolver(ObjectProvider<HttpServletRequest> requestProvider) {
        this.requestProvider = requestProvider;
    }

    public RequestAuditMetadata resolve() {
        var request = requestProvider.getIfAvailable();
        if (request == null) {
            return new RequestAuditMetadata(null, null, null, null, null, null, null, null);
        }
        return new RequestAuditMetadata(
                header(request, WOODY_USER_ID, LEGACY_USER_ID),
                header(request, WOODY_USERNAME, LEGACY_USERNAME),
                header(request, WOODY_EMAIL, LEGACY_EMAIL),
                header(request, WOODY_REALM, LEGACY_REALM),
                header(request, WOODY_REQUEST_ID, LEGACY_REQUEST_ID, REQUEST_ID),
                header(request, WOODY_REQUEST_DEADLINE, LEGACY_REQUEST_DEADLINE, REQUEST_DEADLINE),
                header(request, TRACE_PARENT),
                header(request, TRACE_STATE)
        );
    }

    private String header(HttpServletRequest request, String... names) {
        for (var name : names) {
            var value = request.getHeader(name);
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }
        return null;
    }
}
