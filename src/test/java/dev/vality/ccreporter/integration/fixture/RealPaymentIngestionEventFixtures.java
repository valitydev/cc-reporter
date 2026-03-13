package dev.vality.ccreporter.integration.fixture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.CashFlowAccount;
import dev.vality.damsel.domain.CurrencyRef;
import dev.vality.damsel.domain.FinalCashFlowAccount;
import dev.vality.damsel.domain.FinalCashFlowPosting;
import dev.vality.damsel.domain.AdditionalTransactionInfo;
import dev.vality.damsel.domain.BankCard;
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
import dev.vality.damsel.domain.PaymentTool;
import dev.vality.damsel.domain.PaymentResourcePayer;
import dev.vality.damsel.domain.PaymentRoute;
import dev.vality.damsel.domain.ProviderCashFlowAccount;
import dev.vality.damsel.domain.ProviderRef;
import dev.vality.damsel.domain.ShopConfigRef;
import dev.vality.damsel.domain.SystemCashFlowAccount;
import dev.vality.damsel.domain.TerminalRef;
import dev.vality.damsel.domain.TransactionInfo;
import dev.vality.damsel.domain.TargetInvoicePaymentStatus;
import dev.vality.damsel.domain.ContactInfo;
import dev.vality.damsel.domain.DisposablePaymentResource;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentCashFlowChanged;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChangePayload;
import dev.vality.damsel.payment_processing.InvoicePaymentRouteChanged;
import dev.vality.damsel.payment_processing.InvoicePaymentSessionChange;
import dev.vality.damsel.payment_processing.InvoicePaymentStarted;
import dev.vality.damsel.payment_processing.InvoicePaymentStatusChanged;
import dev.vality.damsel.payment_processing.SessionChangePayload;
import dev.vality.damsel.payment_processing.SessionTransactionBound;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.msgpack.Value;
import org.apache.thrift.TBase;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Загружает санитизированный production-like payment flow из test resources
 * и превращает его в MachineEvent batch.
 */
public final class RealPaymentIngestionEventFixtures {

    public static final String PAYMENT_INVOICE_ID = "2EnbPdxImPo";
    public static final String PAYMENT_ID = "1";

    private static final String RESOURCE_NAME = "2EnbPdxImPo_events.txt";
    private static final int INITIAL_PROVIDER_ID = 254;
    private static final int INITIAL_TERMINAL_ID = 2550;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ThriftSerializer<TBase<?, ?>> THRIFT_SERIALIZER = new ThriftSerializer<>();

    private RealPaymentIngestionEventFixtures() {
    }

    public static List<MachineEvent> paymentEvents() {
        try {
            var root = (ArrayNode) OBJECT_MAPPER.readTree(extractJsonArray(readResource()));
            var machineEvents = new ArrayList<MachineEvent>(root.size());
            for (JsonNode eventNode : root) {
                buildPayload(eventNode).ifPresent(payload ->
                        machineEvents.add(new MachineEvent()
                                .setEventId(eventNode.path("id").asLong())
                                .setSourceId(eventNode.path("source").path("invoice_id").asText())
                                .setSourceNs("payments")
                                .setCreatedAt(eventNode.path("created_at").asText())
                                .setData(Value.bin(serialize(payload))))
                );
            }
            return machineEvents;
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to load real payment ingestion fixture", ex);
        }
    }

    private static byte[] serialize(TBase<?, ?> payload) {
        return THRIFT_SERIALIZER.serialize("", payload);
    }

    private static java.util.Optional<EventPayload> buildPayload(JsonNode eventNode) {
        var invoiceChangesNode = eventNode.path("payload").path("invoice_changes");
        var invoiceChanges = new ArrayList<InvoiceChange>();
        for (JsonNode invoiceChangeNode : invoiceChangesNode) {
            buildInvoiceChange(invoiceChangeNode).ifPresent(invoiceChanges::add);
        }
        if (invoiceChanges.isEmpty()) {
            return java.util.Optional.empty();
        }
        var eventPayload = new EventPayload();
        eventPayload.setInvoiceChanges(invoiceChanges);
        return java.util.Optional.of(eventPayload);
    }

    private static java.util.Optional<InvoiceChange> buildInvoiceChange(JsonNode invoiceChangeNode) {
        var paymentChangeNode = invoiceChangeNode.get("invoice_payment_change");
        if (paymentChangeNode == null) {
            return java.util.Optional.empty();
        }

        var payloadNode = paymentChangeNode.path("payload");
        InvoicePaymentChangePayload changePayload = null;
        if (payloadNode.has("invoice_payment_started")) {
            changePayload = new InvoicePaymentChangePayload();
            changePayload.setInvoicePaymentStarted(buildStarted(payloadNode.path("invoice_payment_started")));
        } else if (payloadNode.has("invoice_payment_route_changed")) {
            changePayload = new InvoicePaymentChangePayload();
            changePayload.setInvoicePaymentRouteChanged(buildRouteChanged(
                    payloadNode.path("invoice_payment_route_changed")
            ));
        } else if (payloadNode.has("invoice_payment_cash_flow_changed")) {
            changePayload = new InvoicePaymentChangePayload();
            changePayload.setInvoicePaymentCashFlowChanged(buildCashFlowChanged(
                    payloadNode.path("invoice_payment_cash_flow_changed")
            ));
        } else if (payloadNode.has("invoice_payment_status_changed")) {
            changePayload = new InvoicePaymentChangePayload();
            changePayload.setInvoicePaymentStatusChanged(buildStatusChanged(
                    payloadNode.path("invoice_payment_status_changed")
            ));
        } else if (isTransactionBoundSessionChange(payloadNode)) {
            changePayload = new InvoicePaymentChangePayload();
            changePayload.setInvoicePaymentSessionChange(buildTransactionBoundSessionChange(
                    payloadNode.path("invoice_payment_session_change")
            ));
        }

        if (changePayload == null) {
            return java.util.Optional.empty();
        }

        var paymentChange = new InvoicePaymentChange();
        paymentChange.setId(paymentChangeNode.path("id").asText());
        paymentChange.setPayload(changePayload);
        var invoiceChange = new InvoiceChange();
        invoiceChange.setInvoicePaymentChange(paymentChange);
        return java.util.Optional.of(invoiceChange);
    }

    private static InvoicePaymentStarted buildStarted(JsonNode startedNode) {
        var paymentNode = startedNode.path("payment");
        var payment = new InvoicePayment();
        payment.setId(paymentNode.path("id").asText());
        payment.setCreatedAt(paymentNode.path("created_at").asText());
        payment.setExternalId(paymentNode.path("external_id").asText(null));
        payment.setStatus(buildStatus(paymentNode.path("status")));
        payment.setCost(buildCash(paymentNode.path("cost")));
        payment.setDomainRevision(paymentNode.path("domain_revision").asLong());
        var partyRef = new PartyConfigRef();
        partyRef.setId(paymentNode.path("party_ref").path("id").asText(null));
        payment.setPartyRef(partyRef);
        var shopRef = new ShopConfigRef();
        shopRef.setId(paymentNode.path("shop_ref").path("id").asText(null));
        payment.setShopRef(shopRef);
        payment.setFlow(buildPaymentFlow());

        if (paymentNode.has("payer")) {
            var paymentResourcePayer = new PaymentResourcePayer();
            paymentResourcePayer.setResource(buildDisposablePaymentResource(paymentNode.path("payer")));
            paymentResourcePayer.setContactInfo(new ContactInfo());
            var payer = new Payer();
            payer.setPaymentResource(paymentResourcePayer);
            payment.setPayer(payer);
        }

        var started = new InvoicePaymentStarted();
        started.setPayment(payment);
        started.setRoute(buildRoute(startedNode.path("route")));
        return started;
    }

    private static InvoicePaymentFlow buildPaymentFlow() {
        var flow = new InvoicePaymentFlow();
        flow.setInstant(new InvoicePaymentFlowInstant());
        return flow;
    }

    private static DisposablePaymentResource buildDisposablePaymentResource(JsonNode payerNode) {
        var paymentToolNode = payerNode.path("payment_resource")
                .path("resource")
                .path("payment_tool")
                .path("bank_card");
        var bankCard = new BankCard();
        bankCard.setToken(paymentToolNode.path("token").asText(null));
        bankCard.setBin(paymentToolNode.path("bin").asText(null));
        bankCard.setLastDigits(paymentToolNode.path("last_digits").asText(null));

        var paymentTool = new PaymentTool();
        paymentTool.setBankCard(bankCard);

        var resource = new DisposablePaymentResource();
        resource.setPaymentTool(paymentTool);
        return resource;
    }

    private static InvoicePaymentRouteChanged buildRouteChanged(JsonNode routeChangedNode) {
        var routeChanged = new InvoicePaymentRouteChanged();
        routeChanged.setRoute(buildRoute(routeChangedNode.path("route")));
        return routeChanged;
    }

    private static InvoicePaymentCashFlowChanged buildCashFlowChanged(JsonNode cashFlowChangedNode) {
        var postings = new ArrayList<FinalCashFlowPosting>();
        for (JsonNode postingNode : cashFlowChangedNode.path("cash_flow")) {
            var posting = new FinalCashFlowPosting();
            posting.setSource(buildCashFlowAccount(postingNode.path("source")));
            posting.setDestination(buildCashFlowAccount(postingNode.path("destination")));
            posting.setVolume(buildCash(postingNode.path("volume")));
            postings.add(posting);
        }
        var cashFlowChanged = new InvoicePaymentCashFlowChanged();
        cashFlowChanged.setCashFlow(postings);
        return cashFlowChanged;
    }

    private static InvoicePaymentStatusChanged buildStatusChanged(JsonNode statusChangedNode) {
        var statusChanged = new InvoicePaymentStatusChanged();
        statusChanged.setStatus(buildStatus(statusChangedNode.path("status")));
        return statusChanged;
    }

    private static InvoicePaymentSessionChange buildTransactionBoundSessionChange(JsonNode sessionChangeNode) {
        var trxNode = sessionChangeNode.path("payload").path("session_transaction_bound").path("trx");
        var transactionInfo = new TransactionInfo();
        transactionInfo.setId(trxNode.path("id").asText());
        transactionInfo.setExtra(Map.of());

        var additionalInfo = new AdditionalTransactionInfo();
        if (trxNode.path("additional_info").has("rrn")) {
            additionalInfo.setRrn(trxNode.path("additional_info").path("rrn").asText());
        }
        if (trxNode.path("additional_info").has("approval_code")) {
            additionalInfo.setApprovalCode(trxNode.path("additional_info").path("approval_code").asText());
        }
        if (additionalInfo.isSetRrn() || additionalInfo.isSetApprovalCode()) {
            transactionInfo.setAdditionalInfo(additionalInfo);
        }

        var transactionBound = new SessionTransactionBound();
        transactionBound.setTrx(transactionInfo);
        var payload = new SessionChangePayload();
        payload.setSessionTransactionBound(transactionBound);
        var target = new TargetInvoicePaymentStatus();
        applyTargetStatus(target, sessionChangeNode.path("target"));
        var sessionChange = new InvoicePaymentSessionChange();
        sessionChange.setTarget(target);
        sessionChange.setPayload(payload);
        return sessionChange;
    }

    private static boolean isTransactionBoundSessionChange(JsonNode payloadNode) {
        return payloadNode.has("invoice_payment_session_change")
                && payloadNode.path("invoice_payment_session_change")
                .path("payload")
                .has("session_transaction_bound");
    }

    private static PaymentRoute buildRoute(JsonNode routeNode) {
        var route = new PaymentRoute();
        var provider = new ProviderRef();
        provider.setId(routeNode.path("provider").path("id").asInt(INITIAL_PROVIDER_ID));
        route.setProvider(provider);
        var terminal = new TerminalRef();
        terminal.setId(routeNode.path("terminal").path("id").asInt(INITIAL_TERMINAL_ID));
        route.setTerminal(terminal);
        return route;
    }

    private static FinalCashFlowAccount buildCashFlowAccount(JsonNode accountNode) {
        var accountTypeNode = accountNode.path("account_type");
        var accountType = new CashFlowAccount();
        if (accountTypeNode.has("provider")) {
            accountType.setProvider(ProviderCashFlowAccount.valueOf(accountTypeNode.path("provider").asText()));
        }
        if (accountTypeNode.has("merchant")) {
            accountType.setMerchant(MerchantCashFlowAccount.valueOf(accountTypeNode.path("merchant").asText()));
        }
        if (accountTypeNode.has("system")) {
            accountType.setSystem(SystemCashFlowAccount.valueOf(accountTypeNode.path("system").asText()));
        }
        var account = new FinalCashFlowAccount();
        account.setAccountType(accountType);
        return account;
    }

    private static Cash buildCash(JsonNode cashNode) {
        var currency = new CurrencyRef();
        currency.setSymbolicCode(cashNode.path("currency").path("symbolic_code").asText());
        var cash = new Cash();
        cash.setAmount(cashNode.path("amount").asLong());
        cash.setCurrency(currency);
        return cash;
    }

    private static InvoicePaymentStatus buildStatus(JsonNode statusNode) {
        var status = new InvoicePaymentStatus();
        var fields = statusNode.fieldNames();
        if (!fields.hasNext()) {
            return status;
        }
        switch (fields.next()) {
            case "pending" -> status.setPending(new InvoicePaymentPending());
            case "processed" -> status.setProcessed(new InvoicePaymentProcessed());
            case "captured" -> status.setCaptured(new InvoicePaymentCaptured());
            default -> {
            }
        }
        return status;
    }

    private static void applyTargetStatus(TargetInvoicePaymentStatus target, JsonNode targetNode) {
        var fields = targetNode.fieldNames();
        if (!fields.hasNext()) {
            target.setProcessed(new InvoicePaymentProcessed());
            return;
        }
        switch (fields.next()) {
            case "processed" -> target.setProcessed(new InvoicePaymentProcessed());
            case "captured" -> target.setCaptured(new InvoicePaymentCaptured());
            default -> target.setProcessed(new InvoicePaymentProcessed());
        }
    }

    private static String readResource() throws IOException {
        try (var inputStream = new ClassPathResource(RESOURCE_NAME).getInputStream()) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String extractJsonArray(String content) {
        var jsonStart = content.indexOf("\n[");
        if (jsonStart >= 0) {
            jsonStart++;
        } else {
            jsonStart = content.indexOf('[');
        }
        if (jsonStart < 0) {
            throw new IllegalArgumentException("Fixture does not contain a JSON array: " + RESOURCE_NAME);
        }
        return content.substring(jsonStart);
    }
}
