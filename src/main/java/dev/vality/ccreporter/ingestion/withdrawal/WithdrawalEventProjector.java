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

import static dev.vality.ccreporter.util.TimestampUtils.toOptionalLocalDateTime;

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
        return Optional.of(baseUpdate(context)
                .setPartyId(withdrawal.getPartyId())
                .setWalletId(withdrawal.getWalletId())
                .setDestinationId(withdrawal.getDestinationId())
                .setCreatedAt(toOptionalLocalDateTime(Instant.parse(withdrawal.getCreatedAt())))
                .setStatus("pending")
                .setProviderId(route != null ? String.valueOf(route.getProviderId()) : null)
                .setTerminalId(route != null ? String.valueOf(route.getTerminalId()) : null)
                .setAmount(body.getAmount())
                .setCurrency(body.getCurrency().getSymbolicCode())
                .setExternalId(withdrawal.getExternalId())
                .setConvertedAmount(quote != null ? quote.getCashTo().getAmount() : null)
                .setExchangeRateInternal(toRate(quote))
                .setProviderAmount(quote != null ? quote.getCashTo().getAmount() : null)
                .setProviderCurrency(quote != null ? quote.getCashTo().getCurrency().getSymbolicCode() : null));
    }

    private Optional<WithdrawalTxnCurrent> routeChangedUpdate(WithdrawalChangeContext context, Change change) {
        if (!change.isSetRoute()) {
            return Optional.empty();
        }
        var route = change.getRoute().getRoute();
        return Optional.of(baseUpdate(context)
                .setProviderId(String.valueOf(route.getProviderId()))
                .setTerminalId(String.valueOf(route.getTerminalId())));
    }

    private Optional<WithdrawalTxnCurrent> statusChangedUpdate(WithdrawalChangeContext context, Change change) {
        if (!change.isSetStatusChanged()) {
            return Optional.empty();
        }
        var status = change.getStatusChanged().getStatus();
        var failure = status.isSetFailed() ? status.getFailed().getFailure() : null;
        var subFailure = failure != null ? failure.getSub() : null;
        return Optional.of(baseUpdate(context)
                .setFinalizedAt(toOptionalLocalDateTime(terminalFinalizedAt(status, context.eventCreatedAt())))
                .setStatus(status.getSetField().getFieldName())
                .setErrorCode(failure != null ? failure.getCode() : null)
                .setErrorReason(failure != null ? failure.getReason() : null)
                .setErrorSubFailure(subFailure != null ? subFailure.getCode() : null));
    }

    private Optional<WithdrawalTxnCurrent> transferCashFlowUpdate(WithdrawalChangeContext context, Change change) {
        if (!change.isSetTransfer()
                || !change.getTransfer().isSetPayload()
                || !change.getTransfer().getPayload().isSetCreated()
                || !change.getTransfer().getPayload().getCreated().isSetTransfer()
                || !change.getTransfer().getPayload().getCreated().getTransfer().isSetCashflow()) {
            return Optional.empty();
        }
        return Optional.of(baseUpdate(context));
    }

    private WithdrawalTxnCurrent baseUpdate(WithdrawalChangeContext context) {
        return new WithdrawalTxnCurrent()
                .setWithdrawalId(context.withdrawalId())
                .setDomainEventId(context.domainEventId())
                .setDomainEventCreatedAt(toOptionalLocalDateTime(context.eventCreatedAt()));
    }

    private record WithdrawalChangeContext(String withdrawalId, long domainEventId, Instant eventCreatedAt) {
    }
}
