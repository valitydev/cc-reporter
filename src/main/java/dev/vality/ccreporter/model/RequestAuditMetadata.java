package dev.vality.ccreporter.model;

public record RequestAuditMetadata(
        String userId,
        String username,
        String email,
        String realm,
        String traceId,
        String traceparent,
        String tracestate
) {
}
