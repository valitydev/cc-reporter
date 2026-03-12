package dev.vality.ccreporter.security;

public record RequestAuditMetadata(
        String userId,
        String username,
        String email,
        String realm,
        String requestId,
        String requestDeadline,
        String traceparent,
        String tracestate
) {
}
