package dev.vality.ccreporter.integration;

import dev.vality.damsel.domain.AdditionalTransactionInfo;
import dev.vality.damsel.domain.BankCard;
import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.CashFlowAccount;
import dev.vality.damsel.domain.ContactInfo;
import dev.vality.damsel.domain.CurrencyRef;
import dev.vality.damsel.domain.DisposablePaymentResource;
import dev.vality.damsel.domain.FinalCashFlowAccount;
import dev.vality.damsel.domain.FinalCashFlowPosting;
import dev.vality.damsel.domain.InvoicePayment;
import dev.vality.damsel.domain.InvoicePaymentCaptured;
import dev.vality.damsel.domain.InvoicePaymentFlow;
import dev.vality.damsel.domain.InvoicePaymentFlowInstant;
import dev.vality.damsel.domain.InvoicePaymentPending;
import dev.vality.damsel.domain.InvoicePaymentProcessed;
import dev.vality.damsel.domain.InvoicePaymentStatus;
import dev.vality.damsel.domain.MerchantCashFlowAccount;
import dev.vality.damsel.domain.PartyConfigRef;
import dev.vality.damsel.domain.Payer;
import dev.vality.damsel.domain.PaymentResourcePayer;
import dev.vality.damsel.domain.PaymentRoute;
import dev.vality.damsel.domain.PaymentTool;
import dev.vality.damsel.domain.ProviderCashFlowAccount;
import dev.vality.damsel.domain.ProviderRef;
import dev.vality.damsel.domain.ShopConfigRef;
import dev.vality.damsel.domain.SystemCashFlowAccount;
import dev.vality.damsel.domain.TargetInvoicePaymentStatus;
import dev.vality.damsel.domain.TerminalRef;
import dev.vality.damsel.domain.TransactionInfo;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentCashFlowChanged;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChangePayload;
import dev.vality.damsel.payment_processing.InvoicePaymentSessionChange;
import dev.vality.damsel.payment_processing.InvoicePaymentStarted;
import dev.vality.damsel.payment_processing.InvoicePaymentStatusChanged;
import dev.vality.damsel.payment_processing.SessionChangePayload;
import dev.vality.damsel.payment_processing.SessionTransactionBound;
import dev.vality.fistful.cashflow.FinalCashFlow;
import dev.vality.fistful.transfer.CreatedChange;
import dev.vality.fistful.transfer.Transfer;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.fistful.withdrawal.QuoteState;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.StatusChange;
import dev.vality.fistful.withdrawal.TransferChange;
import dev.vality.fistful.withdrawal.Withdrawal;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.msgpack.Value;
import java.util.List;
import java.util.Map;
import org.apache.thrift.TBase;

final class SerializedIngestionEventFixtures {

    static final String PAYMENT_INVOICE_ID = "invoice-serialized";
    static final String PAYMENT_ID = "payment-serialized";
    static final String WITHDRAWAL_ID = "withdrawal-serialized";
    static final String WITHDRAWAL_SESSION_ID = "session-serialized";

    private static final ThriftSerializer THRIFT_SERIALIZER = new ThriftSerializer();

    private SerializedIngestionEventFixtures() {
    }

    static List<MachineEvent> paymentEvents() {
        return List.of(
                paymentMachineEvent(1L, startedPayload()),
                paymentMachineEvent(2L, cashFlowChangedPayload()),
                paymentMachineEvent(3L, transactionBoundPayload()),
                paymentMachineEvent(4L, statusChangedPayload())
        );
    }

    static List<MachineEvent> withdrawalEvents() {
        return List.of(
                withdrawalMachineEvent(1L, withdrawalCreatedEvent()),
                withdrawalMachineEvent(2L, withdrawalTransferEvent()),
                withdrawalMachineEvent(5L, withdrawalStatusEvent())
        );
    }

    static List<MachineEvent> withdrawalSessionEvents() {
        return List.of(
                withdrawalSessionMachineEvent(3L, withdrawalSessionCreatedEvent()),
                withdrawalSessionMachineEvent(4L, withdrawalSessionTransactionBoundEvent())
        );
    }

    private static MachineEvent paymentMachineEvent(long eventId, EventPayload payload) {
        return new MachineEvent()
                .setEventId(eventId)
                .setSourceId(PAYMENT_INVOICE_ID)
                .setSourceNs("payments")
                .setCreatedAt("2026-01-01T00:0" + eventId + ":00Z")
                .setData(Value.bin(serialize(payload)));
    }

    private static MachineEvent withdrawalMachineEvent(long eventId, Event payload) {
        return new MachineEvent()
                .setEventId(eventId)
                .setSourceId(WITHDRAWAL_ID)
                .setSourceNs("withdrawals")
                .setCreatedAt("2026-01-01T00:0" + eventId + ":00Z")
                .setData(Value.bin(serialize(payload)));
    }

    private static MachineEvent withdrawalSessionMachineEvent(
            long eventId,
            dev.vality.fistful.withdrawal_session.Event payload
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

    private static EventPayload startedPayload() {
        PartyConfigRef partyRef = new PartyConfigRef();
        partyRef.setId("party-serialized");

        ShopConfigRef shopRef = new ShopConfigRef();
        shopRef.setId("shop-serialized");

        CurrencyRef currency = new CurrencyRef();
        currency.setSymbolicCode("RUB");

        Cash cost = new Cash();
        cost.setAmount(1000L);
        cost.setCurrency(currency);

        InvoicePaymentStatus status = new InvoicePaymentStatus();
        status.setPending(new InvoicePaymentPending());

        InvoicePayment payment = new InvoicePayment();
        payment.setId(PAYMENT_ID);
        payment.setPartyRef(partyRef);
        payment.setShopRef(shopRef);
        payment.setCreatedAt("2026-01-01T00:00:00Z");
        payment.setExternalId("external-payment-1");
        payment.setStatus(status);
        payment.setCost(cost);
        payment.setDomainRevision(1L);
        payment.setFlow(invoicePaymentFlow());
        payment.setPayer(invoicePaymentPayer());

        ProviderRef providerRef = new ProviderRef();
        providerRef.setId(100);

        TerminalRef terminalRef = new TerminalRef();
        terminalRef.setId(200);

        PaymentRoute route = new PaymentRoute();
        route.setProvider(providerRef);
        route.setTerminal(terminalRef);

        InvoicePaymentStarted started = new InvoicePaymentStarted();
        started.setPayment(payment);
        started.setRoute(route);

        InvoicePaymentChangePayload changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentStarted(started);

        InvoicePaymentChange paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        InvoiceChange invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        EventPayload eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload cashFlowChangedPayload() {
        CashFlowAccount providerAccountType = new CashFlowAccount();
        providerAccountType.setProvider(ProviderCashFlowAccount.settlement);
        FinalCashFlowAccount providerAccount = new FinalCashFlowAccount();
        providerAccount.setAccountType(providerAccountType);

        CashFlowAccount merchantAccountType = new CashFlowAccount();
        merchantAccountType.setMerchant(MerchantCashFlowAccount.settlement);
        FinalCashFlowAccount merchantAccount = new FinalCashFlowAccount();
        merchantAccount.setAccountType(merchantAccountType);

        CurrencyRef rub = new CurrencyRef();
        rub.setSymbolicCode("RUB");

        Cash amountCash = new Cash();
        amountCash.setAmount(1000L);
        amountCash.setCurrency(rub);

        FinalCashFlowPosting amountPosting = new FinalCashFlowPosting();
        amountPosting.setSource(providerAccount);
        amountPosting.setDestination(merchantAccount);
        amountPosting.setVolume(amountCash);

        CashFlowAccount systemAccountType = new CashFlowAccount();
        systemAccountType.setSystem(SystemCashFlowAccount.settlement);
        FinalCashFlowAccount systemAccount = new FinalCashFlowAccount();
        systemAccount.setAccountType(systemAccountType);

        Cash feeCash = new Cash();
        feeCash.setAmount(10L);
        feeCash.setCurrency(rub.deepCopy());

        FinalCashFlowPosting feePosting = new FinalCashFlowPosting();
        feePosting.setSource(merchantAccount.deepCopy());
        feePosting.setDestination(systemAccount);
        feePosting.setVolume(feeCash);

        InvoicePaymentCashFlowChanged cashFlowChanged = new InvoicePaymentCashFlowChanged();
        cashFlowChanged.setCashFlow(List.of(amountPosting, feePosting));

        InvoicePaymentChangePayload changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentCashFlowChanged(cashFlowChanged);

        InvoicePaymentChange paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        InvoiceChange invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        EventPayload eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload transactionBoundPayload() {
        AdditionalTransactionInfo additionalInfo = new AdditionalTransactionInfo();
        additionalInfo.setRrn("rrn-payment-1");
        additionalInfo.setApprovalCode("approval-payment-1");

        TransactionInfo transactionInfo = new TransactionInfo();
        transactionInfo.setId("trx-payment-1");
        transactionInfo.setExtra(Map.of());
        transactionInfo.setAdditionalInfo(additionalInfo);

        SessionTransactionBound transactionBound = new SessionTransactionBound();
        transactionBound.setTrx(transactionInfo);

        SessionChangePayload sessionChangePayload = new SessionChangePayload();
        sessionChangePayload.setSessionTransactionBound(transactionBound);

        TargetInvoicePaymentStatus targetStatus = new TargetInvoicePaymentStatus();
        targetStatus.setProcessed(new InvoicePaymentProcessed());

        InvoicePaymentSessionChange sessionChange = new InvoicePaymentSessionChange();
        sessionChange.setTarget(targetStatus);
        sessionChange.setPayload(sessionChangePayload);

        InvoicePaymentChangePayload changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentSessionChange(sessionChange);

        InvoicePaymentChange paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        InvoiceChange invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        EventPayload eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload statusChangedPayload() {
        InvoicePaymentStatus status = new InvoicePaymentStatus();
        status.setCaptured(new InvoicePaymentCaptured());

        InvoicePaymentStatusChanged statusChanged = new InvoicePaymentStatusChanged();
        statusChanged.setStatus(status);

        InvoicePaymentChangePayload changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentStatusChanged(statusChanged);

        InvoicePaymentChange paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        InvoiceChange invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        EventPayload eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static Event withdrawalCreatedEvent() {
        dev.vality.fistful.base.CurrencyRef rub = new dev.vality.fistful.base.CurrencyRef();
        rub.setSymbolicCode("RUB");

        dev.vality.fistful.base.Cash body = new dev.vality.fistful.base.Cash();
        body.setAmount(1000L);
        body.setCurrency(rub);

        Route route = new Route();
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

        QuoteState quoteState = new QuoteState();
        quoteState.setCashFrom(cashFrom);
        quoteState.setCashTo(cashTo);
        quoteState.setCreatedAt("2026-01-01T00:00:00Z");
        quoteState.setExpiresOn("2026-01-01T01:00:00Z");

        Withdrawal withdrawal = new Withdrawal();
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

        Change change = new Change();
        change.setCreated(createdChange);

        Event event = new Event();
        event.setEventId(1L);
        event.setOccuredAt("2026-01-01T00:01:00Z");
        event.setChange(change);
        return event;
    }

    private static Event withdrawalTransferEvent() {
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

        FinalCashFlow finalCashFlow = new FinalCashFlow();
        finalCashFlow.setPostings(List.of(feePosting));

        Transfer transfer = new Transfer();
        transfer.setId("transfer-serialized");
        transfer.setCashflow(finalCashFlow);

        CreatedChange createdChange = new CreatedChange();
        createdChange.setTransfer(transfer);

        dev.vality.fistful.transfer.Change transferPayload = new dev.vality.fistful.transfer.Change();
        transferPayload.setCreated(createdChange);

        TransferChange transferChange = new TransferChange();
        transferChange.setPayload(transferPayload);

        Change change = new Change();
        change.setTransfer(transferChange);

        Event event = new Event();
        event.setEventId(2L);
        event.setOccuredAt("2026-01-01T00:02:00Z");
        event.setChange(change);
        return event;
    }

    private static Event withdrawalStatusEvent() {
        Status status = new Status();
        status.setSucceeded(new dev.vality.fistful.withdrawal.status.Succeeded());

        StatusChange statusChange = new StatusChange();
        statusChange.setStatus(status);

        Change change = new Change();
        change.setStatusChanged(statusChange);

        Event event = new Event();
        event.setEventId(5L);
        event.setOccuredAt("2026-01-01T00:05:00Z");
        event.setChange(change);
        return event;
    }

    private static dev.vality.fistful.withdrawal_session.Event withdrawalSessionCreatedEvent() {
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

        dev.vality.fistful.withdrawal_session.Session session =
                new dev.vality.fistful.withdrawal_session.Session();
        session.setId(WITHDRAWAL_SESSION_ID);
        session.setRoute(route);
        session.setStatus(sessionStatus);
        session.setWithdrawal(withdrawal);

        dev.vality.fistful.withdrawal_session.Change change =
                new dev.vality.fistful.withdrawal_session.Change();
        change.setCreated(session);

        dev.vality.fistful.withdrawal_session.Event event =
                new dev.vality.fistful.withdrawal_session.Event();
        event.setSequence(3);
        event.setOccuredAt("2026-01-01T00:03:00Z");
        event.setChanges(List.of(change));
        return event;
    }

    private static dev.vality.fistful.withdrawal_session.Event withdrawalSessionTransactionBoundEvent() {
        dev.vality.fistful.base.AdditionalTransactionInfo additionalInfo =
                new dev.vality.fistful.base.AdditionalTransactionInfo();
        additionalInfo.setRrn("rrn-withdrawal-1");

        dev.vality.fistful.base.TransactionInfo trxInfo = new dev.vality.fistful.base.TransactionInfo();
        trxInfo.setId("trx-withdrawal-1");
        trxInfo.setExtra(Map.of());
        trxInfo.setAdditionalInfo(additionalInfo);

        dev.vality.fistful.withdrawal_session.TransactionBoundChange transactionBoundChange =
                new dev.vality.fistful.withdrawal_session.TransactionBoundChange();
        transactionBoundChange.setTrxInfo(trxInfo);

        dev.vality.fistful.withdrawal_session.Change change =
                new dev.vality.fistful.withdrawal_session.Change();
        change.setTransactionBound(transactionBoundChange);

        dev.vality.fistful.withdrawal_session.Event event =
                new dev.vality.fistful.withdrawal_session.Event();
        event.setSequence(4);
        event.setOccuredAt("2026-01-01T00:04:00Z");
        event.setChanges(List.of(change));
        return event;
    }

    private static InvoicePaymentFlow invoicePaymentFlow() {
        InvoicePaymentFlow flow = new InvoicePaymentFlow();
        flow.setInstant(new InvoicePaymentFlowInstant());
        return flow;
    }

    private static Payer invoicePaymentPayer() {
        BankCard bankCard = new BankCard();
        bankCard.setToken("token-payment");
        bankCard.setBin("411111");
        bankCard.setLastDigits("1111");

        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setBankCard(bankCard);

        DisposablePaymentResource resource = new DisposablePaymentResource();
        resource.setPaymentTool(paymentTool);

        PaymentResourcePayer paymentResourcePayer = new PaymentResourcePayer();
        paymentResourcePayer.setResource(resource);
        paymentResourcePayer.setContactInfo(new ContactInfo());

        Payer payer = new Payer();
        payer.setPaymentResource(paymentResourcePayer);
        return payer;
    }
}
