package dev.vality.ccreporter.constants;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ReportAuditEventType {

    REPORT_CREATED("report_created"),
    REPORT_CANCELED("report_canceled"),
    PRESIGNED_URL_GENERATED("presigned_url_generated");

    private final String eventType;

}
