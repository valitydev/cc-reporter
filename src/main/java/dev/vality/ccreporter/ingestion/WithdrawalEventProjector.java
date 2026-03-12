package dev.vality.ccreporter.ingestion;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.Failure;
import dev.vality.fistful.base.SubFailure;
import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.QuoteState;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.machinegun.eventsink.MachineEvent;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class WithdrawalEventProjector {

    public List<WithdrawalCurrentUpdate> project(MachineEvent event, Event payload) {
        List<WithdrawalCurrentUpdate> updates = new ArrayList<>();
        if (payload == null || payload.getChange() == null) {
            return updates;
        }
        projectChange(event, payload.getChange(), Instant.parse(payload.getOccuredAt())).ifPresent(updates::add);
        return updates;
    }

    private Optional<WithdrawalCurrentUpdate> projectChange(MachineEvent event, Change change, Instant occurredAt) {
        Instant eventCreatedAt = Instant.parse(event.getCreatedAt());
        String withdrawalId = event.getSourceId();

        if (change.isSetCreated()) {
            var withdrawal = change.getCreated().getWithdrawal();
            Cash body = withdrawal.getBody();
            Route route = withdrawal.getRoute();
            QuoteState quote = withdrawal.getQuote();
            return Optional.of(new WithdrawalCurrentUpdate(
                    withdrawalId,
                    event.getEventId(),
                    eventCreatedAt,
                    withdrawal.getPartyId(),
                    withdrawal.getWalletId(),
                    null,
                    withdrawal.getDestinationId(),
                    Instant.parse(withdrawal.getCreatedAt()),
                    null,
                    "pending",
                    route != null ? String.valueOf(route.getProviderId()) : null,
                    null,
                    route != null ? String.valueOf(route.getTerminalId()) : null,
                    null,
                    body.getAmount(),
                    null,
                    body.getCurrency().getSymbolicCode(),
                    null,
                    withdrawal.getExternalId(),
                    null,
                    null,
                    null,
                    quote != null ? quote.getCashFrom().getAmount() : null,
                    quote != null ? quote.getCashFrom().getCurrency().getSymbolicCode() : null,
                    quote != null ? quote.getCashTo().getAmount() : null,
                    toRate(quote),
                    quote != null ? quote.getCashTo().getAmount() : null,
                    quote != null ? quote.getCashTo().getCurrency().getSymbolicCode() : null
            ));
        }

        if (change.isSetRoute()) {
            Route route = change.getRoute().getRoute();
            return Optional.of(new WithdrawalCurrentUpdate(
                    withdrawalId, event.getEventId(), eventCreatedAt, null, null, null, null, null, null,
                    null,
                    String.valueOf(route.getProviderId()),
                    null,
                    String.valueOf(route.getTerminalId()),
                    null,
                    null, null, null, null, null, null, null, null, null, null, null, null, null, null
            ));
        }

        if (change.isSetStatusChanged()) {
            Status status = change.getStatusChanged().getStatus();
            Failure failure = status.isSetFailed() ? status.getFailed().getFailure() : null;
            SubFailure subFailure = failure != null ? failure.getSub() : null;
            return Optional.of(new WithdrawalCurrentUpdate(
                    withdrawalId, event.getEventId(), eventCreatedAt, null, null, null, null, null,
                    terminalFinalizedAt(status, eventCreatedAt), status.getSetField().getFieldName(), null, null, null,
                    null, null, null, null, null, null,
                    failure != null ? failure.getCode() : null,
                    failure != null ? failure.getReason() : null,
                    subFailure != null ? subFailure.getCode() : null,
                    null, null, null, null, null, null
            ));
        }

        if (change.isSetTransfer()
                && change.getTransfer().isSetPayload()
                && change.getTransfer().getPayload().isSetCreated()
                && change.getTransfer().getPayload().getCreated().isSetTransfer()
                && change.getTransfer().getPayload().getCreated().getTransfer().isSetCashflow()) {
            List<FinalCashFlowPosting> postings = change.getTransfer().getPayload().getCreated().getTransfer().getCashflow()
                    .getPostings();
            return Optional.of(new WithdrawalCurrentUpdate(
                    withdrawalId,
                    event.getEventId(),
                    eventCreatedAt,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    WithdrawalCashFlowExtractor.extractFee(postings),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
            ));
        }

        return Optional.empty();
    }

    private BigDecimal toRate(QuoteState quote) {
        if (quote == null || quote.getCashFrom().getAmount() == 0) {
            return null;
        }
        return BigDecimal.valueOf(quote.getCashTo().getAmount())
                .divide(BigDecimal.valueOf(quote.getCashFrom().getAmount()), 10, RoundingMode.HALF_UP);
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
}
