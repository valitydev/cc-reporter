package dev.vality.ccreporter.integration.fixture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.CurrencyRef;
import dev.vality.fistful.cashflow.FinalCashFlow;
import dev.vality.fistful.cashflow.FinalCashFlowAccount;
import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.transfer.CreatedChange;
import dev.vality.fistful.transfer.Transfer;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.Event;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.RouteChange;
import dev.vality.fistful.withdrawal.StatusChange;
import dev.vality.fistful.withdrawal.TransferChange;
import dev.vality.fistful.withdrawal.Withdrawal;
import dev.vality.fistful.withdrawal.status.Pending;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.fistful.withdrawal.status.Succeeded;
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
import java.util.Optional;

/**
 * Загружает санитизированный production-like withdrawal flow из test resources
 * и превращает его в MachineEvent batch.
 */
public final class RealWithdrawalIngestionEventFixtures {

    public static final String WITHDRAWAL_ID = "211890";

    private static final String RESOURCE_NAME = "withdrawals/211890_events.txt";
    private static final List<String> COLLECTION_RESOURCE_NAMES = List.of(
            "withdrawals/211890_events.txt",
            "withdrawals/257060.txt",
            "withdrawals/257072.txt",
            "withdrawals/257077.txt",
            "withdrawals/257080.txt",
            "withdrawals/257085.txt"
    );
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ThriftSerializer<TBase<?, ?>> THRIFT_SERIALIZER = new ThriftSerializer<>();

    private RealWithdrawalIngestionEventFixtures() {
    }

    public static List<MachineEvent> withdrawalEvents() {
        return withdrawalEvents(RESOURCE_NAME);
    }

    private static List<MachineEvent> withdrawalEvents(String resourceName) {
        try {
            var root = (ArrayNode) OBJECT_MAPPER.readTree(readResource(resourceName));
            var sourceId = detectWithdrawalId(root, resourceName);
            var machineEvents = new ArrayList<MachineEvent>(root.size());
            for (JsonNode eventNode : root) {
                buildEvent(eventNode).ifPresent(payload ->
                        machineEvents.add(new MachineEvent()
                                .setEventId(eventNode.path("event_id").asLong())
                                .setSourceId(sourceId)
                                .setSourceNs("withdrawals")
                                .setCreatedAt(eventNode.path("occured_at").asText())
                                .setData(Value.bin(serialize(payload))))
                );
            }
            return machineEvents;
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to load real withdrawal ingestion fixture", ex);
        }
    }

    public static List<MachineEvent> withdrawalCollectionEvents() {
        return COLLECTION_RESOURCE_NAMES.stream()
                .flatMap(resourceName -> withdrawalEvents(resourceName).stream())
                .toList();
    }

    private static Optional<Event> buildEvent(JsonNode eventNode) {
        var changeNode = eventNode.path("change");
        Change change = null;

        if (changeNode.has("created")) {
            change = new Change();
            change.setCreated(buildCreated(changeNode.path("created")));
        } else if (changeNode.has("route")) {
            change = new Change();
            change.setRoute(buildRouteChange(changeNode.path("route")));
        } else if (changeNode.has("status_changed")) {
            change = new Change();
            change.setStatusChanged(buildStatusChanged(changeNode.path("status_changed")));
        } else if (hasTransferCashFlow(changeNode)) {
            change = new Change();
            change.setTransfer(buildTransferChange(changeNode.path("transfer")));
        }

        if (change == null) {
            return Optional.empty();
        }

        var event = new Event();
        event.setEventId(eventNode.path("event_id").asLong());
        event.setOccuredAt(eventNode.path("occured_at").asText());
        event.setChange(change);
        return Optional.of(event);
    }

    private static dev.vality.fistful.withdrawal.CreatedChange buildCreated(JsonNode createdNode) {
        var withdrawalNode = createdNode.path("withdrawal");
        var withdrawal = new Withdrawal();
        withdrawal.setId(withdrawalNode.path("id").asText());
        withdrawal.setPartyId(withdrawalNode.path("party_id").asText(null));
        withdrawal.setWalletId(withdrawalNode.path("wallet_id").asText(null));
        withdrawal.setDestinationId(withdrawalNode.path("destination_id").asText(null));
        withdrawal.setCreatedAt(withdrawalNode.path("created_at").asText());
        withdrawal.setDomainRevision(withdrawalNode.path("domain_revision").asLong());
        withdrawal.setExternalId(withdrawalNode.path("external_id").asText(null));
        withdrawal.setBody(buildCash(withdrawalNode.path("body")));

        var created = new dev.vality.fistful.withdrawal.CreatedChange();
        created.setWithdrawal(withdrawal);
        return created;
    }

    private static RouteChange buildRouteChange(JsonNode routeNode) {
        var route = new Route();
        route.setProviderId(routeNode.path("route").path("provider_id").asInt());
        route.setTerminalId(routeNode.path("route").path("terminal_id").asInt());
        var routeChange = new RouteChange();
        routeChange.setRoute(route);
        return routeChange;
    }

    private static StatusChange buildStatusChanged(JsonNode statusChangedNode) {
        var statusNode = statusChangedNode.path("status");
        var status = new Status();
        if (statusNode.has("succeeded")) {
            status.setSucceeded(new Succeeded());
        } else if (statusNode.has("pending")) {
            status.setPending(new Pending());
        }
        var statusChange = new StatusChange();
        statusChange.setStatus(status);
        return statusChange;
    }

    private static TransferChange buildTransferChange(JsonNode transferNode) {
        var postingsNode = transferNode.path("payload")
                .path("created")
                .path("transfer")
                .path("cashflow")
                .path("postings");

        var postings = new ArrayList<FinalCashFlowPosting>(postingsNode.size());
        for (JsonNode postingNode : postingsNode) {
            var posting = new FinalCashFlowPosting();
            posting.setSource(buildAccount(postingNode.path("source")));
            posting.setDestination(buildAccount(postingNode.path("destination")));
            posting.setVolume(buildCash(postingNode.path("volume")));
            postings.add(posting);
        }

        var cashFlow = new FinalCashFlow();
        cashFlow.setPostings(postings);

        var transfer = new Transfer();
        transfer.setId(transferNode.path("payload").path("created").path("transfer").path("id").asText());
        transfer.setCashflow(cashFlow);

        var created = new CreatedChange();
        created.setTransfer(transfer);

        var payload = new dev.vality.fistful.transfer.Change();
        payload.setCreated(created);

        var transferChange = new TransferChange();
        transferChange.setPayload(payload);
        return transferChange;
    }

    private static FinalCashFlowAccount buildAccount(JsonNode accountNode) {
        var account = new FinalCashFlowAccount();
        var accountType = new dev.vality.fistful.cashflow.CashFlowAccount();
        var accountTypeNode = accountNode.path("account_type");
        if (accountTypeNode.has("wallet")) {
            accountType.setWallet(dev.vality.fistful.cashflow.WalletCashFlowAccount.valueOf(
                    accountTypeNode.path("wallet").asText()
            ));
        } else if (accountTypeNode.has("system")) {
            accountType.setSystem(dev.vality.fistful.cashflow.SystemCashFlowAccount.valueOf(
                    accountTypeNode.path("system").asText()
            ));
        } else if (accountTypeNode.has("provider")) {
            accountType.setProvider(dev.vality.fistful.cashflow.ProviderCashFlowAccount.valueOf(
                    accountTypeNode.path("provider").asText()
            ));
        }
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

    private static boolean hasTransferCashFlow(JsonNode changeNode) {
        return changeNode.has("transfer")
                && changeNode.path("transfer").path("payload").has("created")
                && changeNode.path("transfer").path("payload").path("created").path("transfer").has("cashflow");
    }

    private static byte[] serialize(TBase<?, ?> payload) {
        return THRIFT_SERIALIZER.serialize("", payload);
    }

    private static String readResource(String resourceName) throws IOException {
        try (var inputStream = new ClassPathResource(resourceName).getInputStream()) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String detectWithdrawalId(ArrayNode root, String resourceName) {
        for (JsonNode eventNode : root) {
            var createdNode = eventNode.path("change").path("created").path("withdrawal").path("id");
            if (!createdNode.isMissingNode()) {
                return createdNode.asText();
            }
        }
        throw new IllegalArgumentException("Failed to detect withdrawal_id from fixture: " + resourceName);
    }
}
