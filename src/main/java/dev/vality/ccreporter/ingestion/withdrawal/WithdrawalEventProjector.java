package dev.vality.ccreporter.ingestion.withdrawal;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.fistful.withdrawal.QuoteState;
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

    public List<WithdrawalTxnCurrent> project(MachineEvent event, Event payload) {
        var updates = new ArrayList<WithdrawalTxnCurrent>();
        if (payload == null || payload.getChange() == null) {
            return updates;
        }
        projectChange(event, payload.getChange()).ifPresent(updates::add);
        return updates;
    }

    private Optional<WithdrawalTxnCurrent> projectChange(MachineEvent event, Change change) {
        var context = new WithdrawalChangeContext(
                event.getSourceId(),
                event.getEventId(),
                Instant.parse(event.getCreatedAt())
        );

        return createdUpdate(context, change)
                .or(() -> routeChangedUpdate(context, change))
                .or(() -> statusChangedUpdate(context, change))
                .or(() -> transferCashFlowUpdate(context, change));
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

    private Optional<WithdrawalTxnCurrent> createdUpdate(WithdrawalChangeContext context, Change change) {
        if (!change.isSetCreated()) {
            return Optional.empty();
        }
        var withdrawal = change.getCreated().getWithdrawal();
        var body = withdrawal.getBody();
        var route = withdrawal.getRoute();
        var quote = withdrawal.getQuote();
        return Optional.of(baseBuilder(context)
                .partyId(withdrawal.getPartyId())
                .walletId(withdrawal.getWalletId())
                .destinationId(withdrawal.getDestinationId())
                .createdAt(Instant.parse(withdrawal.getCreatedAt()))
                .status("pending")
                .providerId(route != null ? String.valueOf(route.getProviderId()) : null)
                .terminalId(route != null ? String.valueOf(route.getTerminalId()) : null)
                .amount(body.getAmount())
                .currency(body.getCurrency().getSymbolicCode())
                .externalId(withdrawal.getExternalId())
                .originalAmount(quote != null ? quote.getCashFrom().getAmount() : null)
                .originalCurrency(quote != null ? quote.getCashFrom().getCurrency().getSymbolicCode() : null)
                .convertedAmount(quote != null ? quote.getCashTo().getAmount() : null)
                .exchangeRateInternal(toRate(quote))
                .providerAmount(quote != null ? quote.getCashTo().getAmount() : null)
                .providerCurrency(quote != null ? quote.getCashTo().getCurrency().getSymbolicCode() : null)
                .build());
    }

    private Optional<WithdrawalTxnCurrent> routeChangedUpdate(WithdrawalChangeContext context, Change change) {
        if (!change.isSetRoute()) {
            return Optional.empty();
        }
        var route = change.getRoute().getRoute();
        return Optional.of(baseBuilder(context)
                .providerId(String.valueOf(route.getProviderId()))
                .terminalId(String.valueOf(route.getTerminalId()))
                .build());
    }

    private Optional<WithdrawalTxnCurrent> statusChangedUpdate(WithdrawalChangeContext context, Change change) {
        if (!change.isSetStatusChanged()) {
            return Optional.empty();
        }
        var status = change.getStatusChanged().getStatus();
        var failure = status.isSetFailed() ? status.getFailed().getFailure() : null;
        var subFailure = failure != null ? failure.getSub() : null;
        return Optional.of(baseBuilder(context)
                .finalizedAt(terminalFinalizedAt(status, context.eventCreatedAt()))
                .status(status.getSetField().getFieldName())
                .errorCode(failure != null ? failure.getCode() : null)
                .errorReason(failure != null ? failure.getReason() : null)
                .errorSubFailure(subFailure != null ? subFailure.getCode() : null)
                .build());
    }

    private Optional<WithdrawalTxnCurrent> transferCashFlowUpdate(WithdrawalChangeContext context, Change change) {
        if (!change.isSetTransfer()
                || !change.getTransfer().isSetPayload()
                || !change.getTransfer().getPayload().isSetCreated()
                || !change.getTransfer().getPayload().getCreated().isSetTransfer()
                || !change.getTransfer().getPayload().getCreated().getTransfer().isSetCashflow()) {
            return Optional.empty();
        }
        var postings = change.getTransfer().getPayload().getCreated().getTransfer().getCashflow().getPostings();
        return Optional.of(baseBuilder(context)
                .fee(WithdrawalCashFlowExtractor.extractFee(postings))
                .build());
    }

    private WithdrawalCurrentUpdateBuilder baseBuilder(WithdrawalChangeContext context) {
        return WithdrawalCurrentUpdateBuilder.builder(
                context.withdrawalId(),
                context.domainEventId(),
                context.eventCreatedAt()
        );
    }

    private record WithdrawalChangeContext(String withdrawalId, long domainEventId, Instant eventCreatedAt) {
    }
}
