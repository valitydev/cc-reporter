package dev.vality.ccreporter.ingestion;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.Failure;
import dev.vality.fistful.base.SubFailure;
import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.fistful.withdrawal.QuoteState;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class WithdrawalEventProjector {

    public List<WithdrawalCurrentUpdate> project(MachineEvent event, Event payload) {
        var updates = new ArrayList<WithdrawalCurrentUpdate>();
        if (payload == null || payload.getChange() == null) {
            return updates;
        }
        projectChange(event, payload.getChange(), Instant.parse(payload.getOccuredAt())).ifPresent(updates::add);
        return updates;
    }

    private Optional<WithdrawalCurrentUpdate> projectChange(MachineEvent event, Change change, Instant occurredAt) {
        var eventCreatedAt = Instant.parse(event.getCreatedAt());
        var withdrawalId = event.getSourceId();

        if (change.isSetCreated()) {
            var withdrawal = change.getCreated().getWithdrawal();
            var body = withdrawal.getBody();
            var route = withdrawal.getRoute();
            var quote = withdrawal.getQuote();
            return Optional.of(new WithdrawalCurrentUpdate(
                    withdrawalId,
                    event.getEventId(),
                    eventCreatedAt,
                    withdrawal.getPartyId(),
                    withdrawal.getWalletId(),
                    null, // TODO CCR-INGESTION: confirm event-native source
                    // for wallet_name/current-state display names.
                    withdrawal.getDestinationId(),
                    Instant.parse(withdrawal.getCreatedAt()),
                    null,
                    "pending",
                    route != null ? String.valueOf(route.getProviderId()) : null,
                    null, // TODO CCR-INGESTION: confirm event-native source for provider_name in current-state.
                    route != null ? String.valueOf(route.getTerminalId()) : null,
                    null, // TODO CCR-INGESTION: confirm event-native source for terminal_name in current-state.
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
            var route = change.getRoute().getRoute();
            return Optional.of(new WithdrawalCurrentUpdate(
                    withdrawalId, event.getEventId(), eventCreatedAt, null, null,
                    null, // TODO CCR-INGESTION: route change still does not populate display names.
                    null, null, null,
                    null,
                    String.valueOf(route.getProviderId()),
                    null, // TODO CCR-INGESTION: confirm provider_name source for route updates.
                    String.valueOf(route.getTerminalId()),
                    null, // TODO CCR-INGESTION: confirm terminal_name source for route updates.
                    null, null, null, null, null, null, null, null, null, null, null, null, null, null
            ));
        }

        if (change.isSetStatusChanged()) {
            var status = change.getStatusChanged().getStatus();
            var failure = status.isSetFailed() ? status.getFailed().getFailure() : null;
            var subFailure = failure != null ? failure.getSub() : null;
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
            var postings =
                    change.getTransfer().getPayload().getCreated().getTransfer().getCashflow()
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
