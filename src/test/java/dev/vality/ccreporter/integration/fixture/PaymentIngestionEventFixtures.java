package dev.vality.ccreporter.integration.fixture;

import dev.vality.damsel.domain.*;
import dev.vality.damsel.domain.InvoicePayment;
import dev.vality.damsel.domain.InvoicePaymentPending;
import dev.vality.damsel.payment_processing.*;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.msgpack.Value;
import org.apache.thrift.TBase;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Собирает payment events в том же виде, в котором ingestion получает их из event sink.
 */
public final class PaymentIngestionEventFixtures {

    public static final String PAYMENT_INVOICE_ID = "invoice-serialized";
    public static final String PAYMENT_ID = "payment-serialized";

    private static final ThriftSerializer<TBase<?, ?>> THRIFT_SERIALIZER = new ThriftSerializer<>();

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

    public static List<MachineEvent> paymentProxyStateFallbackEvents() {
        return List.of(
                paymentMachineEvent(1L, startedPayload()),
                paymentMachineEvent(2L, proxyStatePayload()),
                paymentMachineEvent(3L, statusChangedPayload())
        );
    }

    public static List<MachineEvent> failedPaymentEvents() {
        return List.of(
                paymentMachineEvent(1L, startedPayload()),
                paymentMachineEvent(2L, failedStatusChangedPayload())
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
        var partyRef = new PartyConfigRef();
        partyRef.setId("party-serialized");

        var shopRef = new ShopConfigRef();
        shopRef.setId("shop-serialized");

        var currency = new CurrencyRef();
        currency.setSymbolicCode("RUB");

        var cost = new Cash();
        cost.setAmount(1000L);
        cost.setCurrency(currency);

        var status = new InvoicePaymentStatus();
        status.setPending(new InvoicePaymentPending());

        var payment = new InvoicePayment();
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

        var providerRef = new ProviderRef();
        providerRef.setId(100);

        var terminalRef = new TerminalRef();
        terminalRef.setId(200);

        var route = new PaymentRoute();
        route.setProvider(providerRef);
        route.setTerminal(terminalRef);

        var started = new InvoicePaymentStarted();
        started.setPayment(payment);
        started.setRoute(route);

        var changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentStarted(started);

        var paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        var invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        var eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload cashFlowChangedPayload() {
        var providerAccountType = new CashFlowAccount();
        providerAccountType.setProvider(ProviderCashFlowAccount.settlement);
        var providerAccount = new FinalCashFlowAccount();
        providerAccount.setAccountType(providerAccountType);

        var merchantAccountType = new CashFlowAccount();
        merchantAccountType.setMerchant(MerchantCashFlowAccount.settlement);
        var merchantAccount = new FinalCashFlowAccount();
        merchantAccount.setAccountType(merchantAccountType);

        var rub = new CurrencyRef();
        rub.setSymbolicCode("RUB");

        var amountCash = new Cash();
        amountCash.setAmount(1000L);
        amountCash.setCurrency(rub);

        var amountPosting = new FinalCashFlowPosting();
        amountPosting.setSource(providerAccount);
        amountPosting.setDestination(merchantAccount);
        amountPosting.setVolume(amountCash);

        var systemAccountType = new CashFlowAccount();
        systemAccountType.setSystem(SystemCashFlowAccount.settlement);
        var systemAccount = new FinalCashFlowAccount();
        systemAccount.setAccountType(systemAccountType);

        var feeCash = new Cash();
        feeCash.setAmount(10L);
        feeCash.setCurrency(rub.deepCopy());

        var feePosting = new FinalCashFlowPosting();
        feePosting.setSource(merchantAccount.deepCopy());
        feePosting.setDestination(systemAccount);
        feePosting.setVolume(feeCash);

        var cashFlowChanged = new InvoicePaymentCashFlowChanged();
        cashFlowChanged.setCashFlow(List.of(amountPosting, feePosting));

        var changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentCashFlowChanged(cashFlowChanged);

        var paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        var invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        var eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload transactionBoundPayload() {
        var additionalInfo = new AdditionalTransactionInfo();
        additionalInfo.setRrn("rrn-payment-1");
        additionalInfo.setApprovalCode("approval-payment-1");

        var transactionInfo = new TransactionInfo();
        transactionInfo.setId("trx-payment-1");
        transactionInfo.setExtra(Map.of(
                "rub_to_eur_converted_amount", "900",
                "rub_to_eur_rate", "0.9000000000"
        ));
        transactionInfo.setAdditionalInfo(additionalInfo);

        var transactionBound = new SessionTransactionBound();
        transactionBound.setTrx(transactionInfo);

        var sessionChangePayload = new SessionChangePayload();
        sessionChangePayload.setSessionTransactionBound(transactionBound);

        var targetStatus = new TargetInvoicePaymentStatus();
        targetStatus.setProcessed(new InvoicePaymentProcessed());

        var sessionChange = new InvoicePaymentSessionChange();
        sessionChange.setTarget(targetStatus);
        sessionChange.setPayload(sessionChangePayload);

        var changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentSessionChange(sessionChange);

        var paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        var invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        var eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload statusChangedPayload() {
        var currency = new CurrencyRef();
        currency.setSymbolicCode("EUR");

        var capturedCost = new Cash();
        capturedCost.setAmount(900L);
        capturedCost.setCurrency(currency);

        var captured = new InvoicePaymentCaptured();
        captured.setCost(capturedCost);

        var status = new InvoicePaymentStatus();
        status.setCaptured(captured);

        var statusChanged = new InvoicePaymentStatusChanged();
        statusChanged.setStatus(status);

        var changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentStatusChanged(statusChanged);

        var paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        var invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        var eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload failedStatusChangedPayload() {
        var subFailure = new SubFailure();
        subFailure.setCode("payment_tool_rejected");

        var failure = new Failure();
        failure.setCode("authorization_failed");
        failure.setReason("'FAILED: REFUSED' - 'Transaction failed'");
        failure.setSub(subFailure);

        var operationFailure = new OperationFailure();
        operationFailure.setFailure(failure);

        var failed = new InvoicePaymentFailed();
        failed.setFailure(operationFailure);

        var status = new InvoicePaymentStatus();
        status.setFailed(failed);

        var statusChanged = new InvoicePaymentStatusChanged();
        statusChanged.setStatus(status);

        var changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentStatusChanged(statusChanged);

        var paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        var invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        var eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static EventPayload proxyStatePayload() {
        var proxyStateChanged = new SessionProxyStateChanged();
        proxyStateChanged.setProxyState("""
                {"nextStep":"CHECK_STATUS","providerTrxId":"trx-from-proxy-state-1"}
                """.getBytes(StandardCharsets.UTF_8));

        var sessionChangePayload = new SessionChangePayload();
        sessionChangePayload.setSessionProxyStateChanged(proxyStateChanged);

        var targetStatus = new TargetInvoicePaymentStatus();
        targetStatus.setProcessed(new InvoicePaymentProcessed());

        var sessionChange = new InvoicePaymentSessionChange();
        sessionChange.setTarget(targetStatus);
        sessionChange.setPayload(sessionChangePayload);

        var changePayload = new InvoicePaymentChangePayload();
        changePayload.setInvoicePaymentSessionChange(sessionChange);

        var paymentChange = new InvoicePaymentChange();
        paymentChange.setId(PAYMENT_ID);
        paymentChange.setPayload(changePayload);

        var invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);

        var eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(List.of(invoiceChange));
        return eventPayload;
    }

    private static InvoicePaymentFlow invoicePaymentFlow() {
        var flow = new InvoicePaymentFlow();
        flow.setInstant(new InvoicePaymentFlowInstant());
        return flow;
    }

    private static Payer invoicePaymentPayer() {
        var bankCard = new BankCard();
        bankCard.setToken("token-payment");
        bankCard.setBin("411111");
        bankCard.setLastDigits("1111");

        var paymentTool = new PaymentTool();
        paymentTool.setBankCard(bankCard);

        var resource = new DisposablePaymentResource();
        resource.setPaymentTool(paymentTool);

        var paymentResourcePayer = new PaymentResourcePayer();
        paymentResourcePayer.setResource(resource);
        paymentResourcePayer.setContactInfo(new ContactInfo());

        var payer = new Payer();
        payer.setPaymentResource(paymentResourcePayer);
        return payer;
    }
}
