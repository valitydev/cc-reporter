package dev.vality.ccreporter.ingestion;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.fistful.withdrawal.QuoteState;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
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
        var eventCreatedAt = Instant.parse(event.getCreatedAt());
        var withdrawalId = event.getSourceId();

        if (change.isSetCreated()) {
            var withdrawal = change.getCreated().getWithdrawal();
            var body = withdrawal.getBody();
            var route = withdrawal.getRoute();
            var quote = withdrawal.getQuote();
            return Optional.of(withdrawalUpdate(
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
            var route = change.getRoute().getRoute();
            return Optional.of(withdrawalUpdate(
                    withdrawalId, event.getEventId(), eventCreatedAt, null, null,
                    null,
                    null, null, null,
                    null,
                    String.valueOf(route.getProviderId()),
                    null,
                    String.valueOf(route.getTerminalId()),
                    null,
                    null, null, null, null, null, null, null, null, null, null, null, null, null, null
            ));
        }

        if (change.isSetStatusChanged()) {
            var status = change.getStatusChanged().getStatus();
            var failure = status.isSetFailed() ? status.getFailed().getFailure() : null;
            var subFailure = failure != null ? failure.getSub() : null;
            return Optional.of(withdrawalUpdate(
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
            return Optional.of(withdrawalUpdate(
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

    private WithdrawalTxnCurrent withdrawalUpdate(
            String withdrawalId,
            long domainEventId,
            Instant domainEventCreatedAt,
            String partyId,
            String walletId,
            String walletName,
            String destinationId,
            Instant createdAt,
            Instant finalizedAt,
            String status,
            String providerId,
            String providerName,
            String terminalId,
            String terminalName,
            Long amount,
            Long fee,
            String currency,
            String trxId,
            String externalId,
            String errorCode,
            String errorReason,
            String errorSubFailure,
            Long originalAmount,
            String originalCurrency,
            Long convertedAmount,
            BigDecimal exchangeRateInternal,
            Long providerAmount,
            String providerCurrency
    ) {
        var update = new WithdrawalTxnCurrent();
        update.setWithdrawalId(withdrawalId);
        update.setDomainEventId(domainEventId);
        update.setDomainEventCreatedAt(toLocalDateTime(domainEventCreatedAt));
        update.setPartyId(partyId);
        update.setWalletId(walletId);
        update.setWalletName(walletName);
        update.setDestinationId(destinationId);
        update.setCreatedAt(toLocalDateTime(createdAt));
        update.setFinalizedAt(toLocalDateTime(finalizedAt));
        update.setStatus(status);
        update.setProviderId(providerId);
        update.setProviderName(providerName);
        update.setTerminalId(terminalId);
        update.setTerminalName(terminalName);
        update.setAmount(amount);
        update.setFee(fee);
        update.setCurrency(currency);
        update.setTrxId(trxId);
        update.setExternalId(externalId);
        update.setErrorCode(errorCode);
        update.setErrorReason(errorReason);
        update.setErrorSubFailure(errorSubFailure);
        update.setOriginalAmount(originalAmount);
        update.setOriginalCurrency(originalCurrency);
        update.setConvertedAmount(convertedAmount);
        update.setExchangeRateInternal(exchangeRateInternal);
        update.setProviderAmount(providerAmount);
        update.setProviderCurrency(providerCurrency);
        return update;
    }

    private LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
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
