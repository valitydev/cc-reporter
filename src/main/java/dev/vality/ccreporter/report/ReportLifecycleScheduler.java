package dev.vality.ccreporter.report;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "ccr.scheduler", name = "enabled", havingValue = "true")
public class ReportLifecycleScheduler {

    private final ReportLifecycleService reportLifecycleService;

    public ReportLifecycleScheduler(ReportLifecycleService reportLifecycleService) {
        this.reportLifecycleService = reportLifecycleService;
    }

    @Scheduled(fixedDelayString = "${ccr.scheduler.poll-interval-ms:10000}")
    public void runLifecycleTick() {
        reportLifecycleService.timeoutStaleProcessingReports();
        reportLifecycleService.expireReadyReports();
        reportLifecycleService.processNextPendingReport();
    }
}
