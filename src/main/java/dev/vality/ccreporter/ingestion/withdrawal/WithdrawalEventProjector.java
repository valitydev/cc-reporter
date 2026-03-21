package dev.vality.ccreporter.ingestion.withdrawal;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.ccreporter.util.DomainCashFlowExtractor;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.QuoteState;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static dev.vality.ccreporter.util.DomainStatusUtils.PENDING_STATUS;
import static dev.vality.ccreporter.util.DomainStatusUtils.extractErrorSummary;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;
import static dev.vality.ccreporter.util.TimestampUtils.toOptionalLocalDateTime;

@Component
public class WithdrawalEventProjector {

    public List<WithdrawalTxnCurrent> project(MachineEvent event, TimestampedChange payload) {
        var updates = new ArrayList<WithdrawalTxnCurrent>();
        if (payload == null || payload.getChange() == null) {
            return updates;
        }
        projectChange(event, payload.getChange()).ifPresent(updates::add);
        return updates;
    }

    private Optional<WithdrawalTxnCurrent> projectChange(MachineEvent event, Change change) {
        return createdUpdate(event, change)
                .or(() -> routeChangedUpdate(event, change))
                .or(() -> statusChangedUpdate(event, change))
                .or(() -> transferCashFlowUpdate(event, change));
    }

    private Optional<WithdrawalTxnCurrent> createdUpdate(MachineEvent event, Change change) {
        if (!change.isSetCreated()) {
            return Optional.empty();
        }
        var withdrawal = change.getCreated().getWithdrawal();
        var body = withdrawal.getBody();
        var route = withdrawal.getRoute();
        var quote = withdrawal.getQuote();

        return Optional.of(baseUpdate(event)
                .setPartyId(withdrawal.getPartyId())
                .setWalletId(withdrawal.getWalletId())
                .setDestinationId(withdrawal.getDestinationId())
                .setCreatedAt(toLocalDateTime(withdrawal.getCreatedAt()))
                .setStatus(PENDING_STATUS)
                .setProviderId(route != null ? String.valueOf(route.getProviderId()) : null)
                .setTerminalId(route != null ? String.valueOf(route.getTerminalId()) : null)
                .setAmount(body.getAmount())
                .setCurrency(body.getCurrency().getSymbolicCode())
                .setExternalId(withdrawal.getExternalId())
                .setOriginalAmount(quote != null ? quote.getCashFrom().getAmount() : null)
                .setOriginalCurrency(quote != null ? quote.getCashFrom().getCurrency().getSymbolicCode() : null)
                .setExchangeRateInternal(toRate(quote))
                .setProviderAmount(quote != null ? quote.getCashTo().getAmount() : null)
                .setProviderCurrency(quote != null ? quote.getCashTo().getCurrency().getSymbolicCode() : null));
    }

    private Optional<WithdrawalTxnCurrent> routeChangedUpdate(MachineEvent event, Change change) {
        if (!change.isSetRoute()) {
            return Optional.empty();
        }
        var route = change.getRoute().getRoute();
        return Optional.of(baseUpdate(event)
                .setProviderId(String.valueOf(route.getProviderId()))
                .setTerminalId(String.valueOf(route.getTerminalId())));
    }

    private Optional<WithdrawalTxnCurrent> statusChangedUpdate(MachineEvent event, Change change) {
        if (!change.isSetStatusChanged()) {
            return Optional.empty();
        }
        var status = change.getStatusChanged().getStatus();
        return Optional.of(baseUpdate(event)
                .setStatus(status.getSetField().getFieldName())
                .setFinalizedAt(
                        toOptionalLocalDateTime(terminalFinalizedAt(status, Instant.parse(event.getCreatedAt()))))
                .setErrorSummary(extractErrorSummary(status)));
    }

    private Optional<WithdrawalTxnCurrent> transferCashFlowUpdate(MachineEvent event, Change change) {
        if (!change.isSetTransfer()
                || !change.getTransfer().isSetPayload()
                || !change.getTransfer().getPayload().isSetCreated()
                || !change.getTransfer().getPayload().getCreated().isSetTransfer()
                || !change.getTransfer().getPayload().getCreated().getTransfer().isSetCashflow()) {
            return Optional.empty();
        }
        var postings = change.getTransfer().getPayload().getCreated().getTransfer().getCashflow().getPostings();
        return Optional.of(baseUpdate(event)
                .setFee(DomainCashFlowExtractor.extractWithdrawalFee(postings)));
    }

    private WithdrawalTxnCurrent baseUpdate(MachineEvent event) {
        return new WithdrawalTxnCurrent()
                .setWithdrawalId(event.getSourceId())
                .setDomainEventId(event.getEventId())
                .setDomainEventCreatedAt(toLocalDateTime(event.getCreatedAt()));
    }

    private Instant terminalFinalizedAt(Status status, Instant eventCreatedAt) {
        if (status == null || status.getSetField() == null) {
            return null;
        }
        return switch (status.getSetField()) {
            case SUCCEEDED, FAILED -> eventCreatedAt;
            default -> null;
        };
    }

    private BigDecimal toRate(QuoteState quote) {
        if (quote == null || quote.getCashFrom().getAmount() == 0) {
            return null;
        }
        return BigDecimal.valueOf(quote.getCashTo().getAmount())
                .divide(BigDecimal.valueOf(quote.getCashFrom().getAmount()), 10, RoundingMode.HALF_UP);
    }
}
