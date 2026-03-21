package dev.vality.ccreporter.fixture;

import dev.vality.ccreporter.serde.thrift.ThriftSerializer;
import dev.vality.fistful.cashflow.FinalCashFlow;
import dev.vality.fistful.transfer.CreatedChange;
import dev.vality.fistful.transfer.Transfer;
import dev.vality.fistful.withdrawal.*;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.fistful.withdrawal_session.Session;
import dev.vality.fistful.withdrawal_session.TransactionBoundChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.msgpack.Value;
import org.apache.thrift.TBase;

import java.util.List;

/**
 * Собирает serialized events для withdrawal и withdrawal-session потока, чтобы эта рутина не жила прямо в тестах.
 */
public final class WithdrawalIngestionEventFixtures {

    public static final String WITHDRAWAL_ID = "withdrawal-serialized";
    public static final String WITHDRAWAL_SOURCE_ID = "withdrawal-serialized";
    public static final String WITHDRAWAL_SESSION_ID = "session-serialized";

    private static final ThriftSerializer<TBase<?, ?>> THRIFT_SERIALIZER = new ThriftSerializer<>();


    private WithdrawalIngestionEventFixtures() {
    }

    public static List<MachineEvent> withdrawalEvents() {
        return List.of(
                withdrawalMachineEvent(1L, withdrawalCreatedTimestampedChange()),
                withdrawalMachineEvent(2L, withdrawalTransferTimestampedChange()),
                withdrawalMachineEvent(5L, withdrawalStatusTimestampedChange())
        );
    }

    public static List<MachineEvent> withdrawalSessionEvents() {
        return List.of(
                withdrawalSessionMachineEvent(3L, withdrawalSessionCreatedEvent()),
                withdrawalSessionMachineEvent(4L, withdrawalSessionTransactionBoundEvent())
        );
    }

    private static MachineEvent withdrawalMachineEvent(long eventId, TimestampedChange payload) {
        return new MachineEvent()
                .setEventId(eventId)
                .setSourceId(WITHDRAWAL_ID)
                .setSourceNs("withdrawals")
                .setCreatedAt("2026-01-01T00:0" + eventId + ":00Z")
                .setData(Value.bin(serialize(payload)));
    }

    private static MachineEvent withdrawalSessionMachineEvent(
            long eventId,
            dev.vality.fistful.withdrawal_session.TimestampedChange payload
    ) {
        return new MachineEvent()
                .setEventId(eventId)
                .setSourceId(WITHDRAWAL_SESSION_ID)
                .setSourceNs("withdrawal-sessions")
                .setCreatedAt("2026-01-01T00:0" + eventId + ":00Z")
                .setData(Value.bin(serialize(payload)));
    }

    private static byte[] serialize(TBase<?, ?> payload) {
        return THRIFT_SERIALIZER.serialize("", payload);
    }

    private static TimestampedChange withdrawalCreatedTimestampedChange() {
        dev.vality.fistful.base.CurrencyRef rub = new dev.vality.fistful.base.CurrencyRef();
        rub.setSymbolicCode("RUB");

        dev.vality.fistful.base.Cash body = new dev.vality.fistful.base.Cash();
        body.setAmount(1000L);
        body.setCurrency(rub);

        var route = new Route();
        route.setProviderId(300);
        route.setTerminalId(400);

        dev.vality.fistful.base.CurrencyRef usd = new dev.vality.fistful.base.CurrencyRef();
        usd.setSymbolicCode("USD");

        dev.vality.fistful.base.Cash cashFrom = new dev.vality.fistful.base.Cash();
        cashFrom.setAmount(1200L);
        cashFrom.setCurrency(usd);

        dev.vality.fistful.base.Cash cashTo = new dev.vality.fistful.base.Cash();
        cashTo.setAmount(1000L);
        cashTo.setCurrency(rub.deepCopy());

        var quoteState = new QuoteState();
        quoteState.setCashFrom(cashFrom);
        quoteState.setCashTo(cashTo);
        quoteState.setCreatedAt("2026-01-01T00:00:00Z");
        quoteState.setExpiresOn("2026-01-01T01:00:00Z");

        var withdrawal = new Withdrawal();
        withdrawal.setId(WITHDRAWAL_ID);
        withdrawal.setPartyId("party-serialized");
        withdrawal.setWalletId("wallet-serialized");
        withdrawal.setDestinationId("destination-serialized");
        withdrawal.setCreatedAt("2026-01-01T00:00:00Z");
        withdrawal.setExternalId("external-withdrawal-1");
        withdrawal.setBody(body);
        withdrawal.setRoute(route);
        withdrawal.setQuote(quoteState);

        dev.vality.fistful.withdrawal.CreatedChange createdChange =
                new dev.vality.fistful.withdrawal.CreatedChange();
        createdChange.setWithdrawal(withdrawal);

        var change = new dev.vality.fistful.withdrawal.Change();
        change.setCreated(createdChange);

        var timestampedChange = new TimestampedChange();
        timestampedChange.setOccuredAt("2026-01-01T00:01:00Z");
        timestampedChange.setChange(change);
        return timestampedChange;
    }

    private static TimestampedChange withdrawalTransferTimestampedChange() {
        dev.vality.fistful.cashflow.CashFlowAccount walletAccountType =
                new dev.vality.fistful.cashflow.CashFlowAccount();
        walletAccountType.setWallet(dev.vality.fistful.cashflow.WalletCashFlowAccount.sender_settlement);
        dev.vality.fistful.cashflow.FinalCashFlowAccount walletAccount =
                new dev.vality.fistful.cashflow.FinalCashFlowAccount();
        walletAccount.setAccountType(walletAccountType);

        dev.vality.fistful.cashflow.CashFlowAccount systemAccountType =
                new dev.vality.fistful.cashflow.CashFlowAccount();
        systemAccountType.setSystem(dev.vality.fistful.cashflow.SystemCashFlowAccount.settlement);
        dev.vality.fistful.cashflow.FinalCashFlowAccount systemAccount =
                new dev.vality.fistful.cashflow.FinalCashFlowAccount();
        systemAccount.setAccountType(systemAccountType);

        dev.vality.fistful.base.CurrencyRef rub = new dev.vality.fistful.base.CurrencyRef();
        rub.setSymbolicCode("RUB");

        dev.vality.fistful.base.Cash feeCash = new dev.vality.fistful.base.Cash();
        feeCash.setAmount(20L);
        feeCash.setCurrency(rub);

        dev.vality.fistful.cashflow.FinalCashFlowPosting feePosting =
                new dev.vality.fistful.cashflow.FinalCashFlowPosting();
        feePosting.setSource(walletAccount);
        feePosting.setDestination(systemAccount);
        feePosting.setVolume(feeCash);

        var finalCashFlow = new FinalCashFlow();
        finalCashFlow.setPostings(List.of(feePosting));

        var transfer = new Transfer();
        transfer.setId("transfer-serialized");
        transfer.setCashflow(finalCashFlow);

        var createdChange = new CreatedChange();
        createdChange.setTransfer(transfer);

        dev.vality.fistful.transfer.Change transferPayload = new dev.vality.fistful.transfer.Change();
        transferPayload.setCreated(createdChange);

        var transferChange = new TransferChange();
        transferChange.setPayload(transferPayload);

        var change = new dev.vality.fistful.withdrawal.Change();
        change.setTransfer(transferChange);

        var timestampedChange = new TimestampedChange();
        timestampedChange.setOccuredAt("2026-01-01T00:02:00Z");
        timestampedChange.setChange(change);
        return timestampedChange;
    }

    private static TimestampedChange withdrawalStatusTimestampedChange() {
        var status = new Status();
        status.setSucceeded(new dev.vality.fistful.withdrawal.status.Succeeded());

        var statusChange = new StatusChange();
        statusChange.setStatus(status);

        var change = new dev.vality.fistful.withdrawal.Change();
        change.setStatusChanged(statusChange);

        var timestampedChange = new TimestampedChange();
        timestampedChange.setOccuredAt("2026-01-01T00:05:00Z");
        timestampedChange.setChange(change);
        return timestampedChange;
    }

    private static dev.vality.fistful.withdrawal_session.TimestampedChange withdrawalSessionCreatedEvent() {
        dev.vality.fistful.withdrawal_session.Route route = new dev.vality.fistful.withdrawal_session.Route();
        route.setProviderId(300);
        route.setTerminalId(400);

        dev.vality.fistful.withdrawal_session.SessionStatus sessionStatus =
                new dev.vality.fistful.withdrawal_session.SessionStatus();
        sessionStatus.setActive(new dev.vality.fistful.withdrawal_session.SessionActive());

        dev.vality.fistful.base.CurrencyRef rub = new dev.vality.fistful.base.CurrencyRef();
        rub.setSymbolicCode("RUB");

        dev.vality.fistful.base.Cash cash = new dev.vality.fistful.base.Cash();
        cash.setAmount(1000L);
        cash.setCurrency(rub);

        dev.vality.fistful.base.BankCard bankCard = new dev.vality.fistful.base.BankCard();
        bankCard.setToken("token");
        bankCard.setBin("411111");
        bankCard.setMaskedPan("411111****1111");

        dev.vality.fistful.base.ResourceBankCard resourceBankCard =
                new dev.vality.fistful.base.ResourceBankCard();
        resourceBankCard.setBankCard(bankCard);

        dev.vality.fistful.base.Resource destinationResource = new dev.vality.fistful.base.Resource();
        destinationResource.setBankCard(resourceBankCard);

        dev.vality.fistful.withdrawal_session.Withdrawal withdrawal =
                new dev.vality.fistful.withdrawal_session.Withdrawal();
        withdrawal.setId(WITHDRAWAL_ID);
        withdrawal.setCash(cash);
        withdrawal.setDestinationResource(destinationResource);

        Session session = new Session();
        session.setId(WITHDRAWAL_SESSION_ID);
        session.setRoute(route);
        session.setStatus(sessionStatus);
        session.setWithdrawal(withdrawal);

        var change = new dev.vality.fistful.withdrawal_session.Change();
        change.setCreated(session);

        dev.vality.fistful.withdrawal_session.TimestampedChange timestampedChange =
                new dev.vality.fistful.withdrawal_session.TimestampedChange();
        timestampedChange.setOccuredAt("2026-01-01T00:03:00Z");
        timestampedChange.setChange(change);
        return timestampedChange;
    }

    private static dev.vality.fistful.withdrawal_session.TimestampedChange withdrawalSessionTransactionBoundEvent() {
        dev.vality.fistful.base.AdditionalTransactionInfo additionalInfo =
                new dev.vality.fistful.base.AdditionalTransactionInfo();
        additionalInfo.setRrn("rrn-withdrawal-1");

        dev.vality.fistful.base.TransactionInfo trxInfo = new dev.vality.fistful.base.TransactionInfo();
        trxInfo.setId("trx-withdrawal-1");
        trxInfo.setExtra(java.util.Map.of());
        trxInfo.setAdditionalInfo(additionalInfo);

        TransactionBoundChange transactionBoundChange = new TransactionBoundChange();
        transactionBoundChange.setTrxInfo(trxInfo);

        var change = new dev.vality.fistful.withdrawal_session.Change();
        change.setTransactionBound(transactionBoundChange);

        dev.vality.fistful.withdrawal_session.TimestampedChange timestampedChange =
                new dev.vality.fistful.withdrawal_session.TimestampedChange();
        timestampedChange.setOccuredAt("2026-01-01T00:04:00Z");
        timestampedChange.setChange(change);
        return timestampedChange;
    }
}
