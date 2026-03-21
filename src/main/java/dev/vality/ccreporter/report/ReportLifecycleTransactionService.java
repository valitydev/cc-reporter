package dev.vality.ccreporter.report;

import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.model.GeneratedCsvReport;
import dev.vality.ccreporter.model.ReportFileMetadata;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ReportLifecycleTransactionService {

    private final ReportLifecycleDao reportLifecycleDao;

    @Transactional
    public void publishCompletedReport(
            long reportId,
            ReportFileMetadata fileMetadata,
            Instant finishedAt,
            Instant expiresAt,
            GeneratedCsvReport generatedCsvReport
    ) {
        var published = reportLifecycleDao.publishFileRecord(reportId, fileMetadata, finishedAt);
        var markedCreated = reportLifecycleDao.markCreated(
                reportId,
                generatedCsvReport.dataSnapshotFixedAt(),
                finishedAt,
                expiresAt,
                generatedCsvReport.rowsCount()
        );
        if (!published || !markedCreated) {
            throw new IllegalStateException("Failed to publish created report " + reportId);
        }
    }
}
