package dev.vality.ccreporter.integration.fixture;

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
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.msgpack.Value;
import org.apache.thrift.TBase;

import java.util.List;
import java.util.Map;

/**
 * Собирает payment events в том же виде, в котором ingestion получает их из event sink.
 */
public final class PaymentIngestionEventFixtures {

    public static final String PAYMENT_INVOICE_ID = "invoice-serialized";
    public static final String PAYMENT_ID = "payment-serialized";

    private static final ThriftSerializer THRIFT_SERIALIZER = new ThriftSerializer();

    private PaymentIngestionEventFixtures() {
    }

    public static List<MachineEvent> paymentEvents() {
        return List.of(
                paymentMachineEvent(1L, startedPayload()),
                paymentMachineEvent(2L, cashFlowChangedPayload()),
                paymentMachineEvent(3L, transactionBoundPayload()),
                paymentMachineEvent(4L, statusChangedPayload())
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
