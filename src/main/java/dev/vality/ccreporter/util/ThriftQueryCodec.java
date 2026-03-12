package dev.vality.ccreporter.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.vality.ccreporter.*;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;

@Component
public class ThriftQueryCodec {

    private final ObjectMapper objectMapper;

    public ThriftQueryCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String serialize(ReportQuery query) {
        try {
            if (query == null) {
                throw new IllegalArgumentException("query is required");
            }
            JsonNode rootNode;
            if (query.isSetPayments()) {
                rootNode = objectMapper.createObjectNode()
                        .set("payments", writePaymentsQuery(query.getPayments()));
            } else if (query.isSetWithdrawals()) {
                rootNode = objectMapper.createObjectNode()
                        .set("withdrawals", writeWithdrawalsQuery(query.getWithdrawals()));
            } else {
                throw new IllegalArgumentException("query must select exactly one branch");
            }
            return objectMapper.writeValueAsString(rootNode);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Failed to serialize report query", ex);
        }
    }

    public ReportQuery deserialize(String queryJson) {
        try {
            var query = new ReportQuery();
            var rootNode = objectMapper.readTree(queryJson);
            if (rootNode.has("payments")) {
                query.setPayments(readPaymentsQuery(rootNode.get("payments")));
            } else if (rootNode.has("withdrawals")) {
                query.setWithdrawals(readWithdrawalsQuery(rootNode.get("withdrawals")));
            } else {
                throw new IllegalStateException("Stored report query must contain payments or withdrawals branch");
            }
            return query;
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to deserialize stored report query", ex);
        }
    }

    public ReportType resolveReportType(ReportQuery query) {
        var payments = query != null && query.isSetPayments();
        var withdrawals = query != null && query.isSetWithdrawals();
        if (payments == withdrawals) {
            return null;
        }
        return payments ? ReportType.payments : ReportType.withdrawals;
    }

    public QueryTimeRange extractTimeRange(ReportQuery query) {
        TimeRange timeRange;
        if (query == null) {
            throw new IllegalArgumentException("query is required");
        }
        if (query.isSetPayments()) {
            var paymentsQuery = query.getPayments();
            timeRange = paymentsQuery == null ? null : paymentsQuery.getTimeRange();
        } else if (query.isSetWithdrawals()) {
            var withdrawalsQuery = query.getWithdrawals();
            timeRange = withdrawalsQuery == null ? null : withdrawalsQuery.getTimeRange();
        } else {
            throw new IllegalArgumentException("query must select exactly one branch");
        }
        if (timeRange == null || !timeRange.isSetFromTime() || !timeRange.isSetToTime()) {
            throw new IllegalArgumentException("time_range with from_time and to_time is required");
        }
        return new QueryTimeRange(
                TimestampUtils.parse(timeRange.getFromTime()),
                TimestampUtils.parse(timeRange.getToTime())
        );
    }

    public String hash(String value) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            var hashBytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            var stringBuilder = new StringBuilder(hashBytes.length * 2);
            for (byte hashByte : hashBytes) {
                stringBuilder.append(String.format("%02x", hashByte));
            }
            return stringBuilder.toString();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to hash report query", ex);
        }
    }

    public record QueryTimeRange(Instant from, Instant to) {
    }

    private ObjectNode writePaymentsQuery(PaymentsQuery query) {
        var node = objectMapper.createObjectNode();
        node.set("time_range", writeTimeRange(query.getTimeRange()));
        putArray(node, "party_ids", query.getPartyIds());
        putArray(node, "shop_ids", query.getShopIds());
        putArray(node, "provider_ids", query.getProviderIds());
        putArray(node, "terminal_ids", query.getTerminalIds());
        putArray(node, "trx_ids", query.getTrxIds());
        putArray(node, "currencies", query.getCurrencies());
        putArray(node, "statuses", query.getStatuses());
        if (query.isSetFilter()) {
            var filterNode = objectMapper.createObjectNode();
            putText(filterNode, "shop_term", query.getFilter().getShopTerm());
            putText(filterNode, "provider_term", query.getFilter().getProviderTerm());
            putText(filterNode, "terminal_term", query.getFilter().getTerminalTerm());
            putText(filterNode, "trx_term", query.getFilter().getTrxTerm());
            node.set("filter", filterNode);
        }
        return node;
    }

    private PaymentsQuery readPaymentsQuery(JsonNode node) {
        var query = new PaymentsQuery();
        query.setTimeRange(readTimeRange(node.get("time_range")));
        setListIfPresent(node, "party_ids", query::setPartyIds);
        setListIfPresent(node, "shop_ids", query::setShopIds);
        setListIfPresent(node, "provider_ids", query::setProviderIds);
        setListIfPresent(node, "terminal_ids", query::setTerminalIds);
        setListIfPresent(node, "trx_ids", query::setTrxIds);
        setListIfPresent(node, "currencies", query::setCurrencies);
        setListIfPresent(node, "statuses", query::setStatuses);
        var filterNode = node.get("filter");
        if (filterNode != null && !filterNode.isNull()) {
            var filter = new PaymentsSearchFilter();
            setTextIfPresent(filterNode, "shop_term", filter::setShopTerm);
            setTextIfPresent(filterNode, "provider_term", filter::setProviderTerm);
            setTextIfPresent(filterNode, "terminal_term", filter::setTerminalTerm);
            setTextIfPresent(filterNode, "trx_term", filter::setTrxTerm);
            query.setFilter(filter);
        }
        return query;
    }

    private ObjectNode writeWithdrawalsQuery(WithdrawalsQuery query) {
        var node = objectMapper.createObjectNode();
        node.set("time_range", writeTimeRange(query.getTimeRange()));
        putArray(node, "party_ids", query.getPartyIds());
        putArray(node, "wallet_ids", query.getWalletIds());
        putArray(node, "provider_ids", query.getProviderIds());
        putArray(node, "terminal_ids", query.getTerminalIds());
        putArray(node, "trx_ids", query.getTrxIds());
        putArray(node, "currencies", query.getCurrencies());
        putArray(node, "statuses", query.getStatuses());
        if (query.isSetFilter()) {
            var filterNode = objectMapper.createObjectNode();
            putText(filterNode, "wallet_term", query.getFilter().getWalletTerm());
            putText(filterNode, "provider_term", query.getFilter().getProviderTerm());
            putText(filterNode, "terminal_term", query.getFilter().getTerminalTerm());
            putText(filterNode, "trx_term", query.getFilter().getTrxTerm());
            node.set("filter", filterNode);
        }
        return node;
    }

    private WithdrawalsQuery readWithdrawalsQuery(JsonNode node) {
        var query = new WithdrawalsQuery();
        query.setTimeRange(readTimeRange(node.get("time_range")));
        setListIfPresent(node, "party_ids", query::setPartyIds);
        setListIfPresent(node, "wallet_ids", query::setWalletIds);
        setListIfPresent(node, "provider_ids", query::setProviderIds);
        setListIfPresent(node, "terminal_ids", query::setTerminalIds);
        setListIfPresent(node, "trx_ids", query::setTrxIds);
        setListIfPresent(node, "currencies", query::setCurrencies);
        setListIfPresent(node, "statuses", query::setStatuses);
        var filterNode = node.get("filter");
        if (filterNode != null && !filterNode.isNull()) {
            var filter = new WithdrawalsSearchFilter();
            setTextIfPresent(filterNode, "wallet_term", filter::setWalletTerm);
            setTextIfPresent(filterNode, "provider_term", filter::setProviderTerm);
            setTextIfPresent(filterNode, "terminal_term", filter::setTerminalTerm);
            setTextIfPresent(filterNode, "trx_term", filter::setTrxTerm);
            query.setFilter(filter);
        }
        return query;
    }

    private ObjectNode writeTimeRange(TimeRange timeRange) {
        var node = objectMapper.createObjectNode();
        putText(node, "from_time", timeRange.getFromTime());
        putText(node, "to_time", timeRange.getToTime());
        return node;
    }

    private TimeRange readTimeRange(JsonNode node) {
        return new TimeRange(node.get("from_time").asText(), node.get("to_time").asText());
    }

    private void putArray(ObjectNode node, String fieldName, List<String> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        var arrayNode = objectMapper.createArrayNode();
        values.forEach(arrayNode::add);
        node.set(fieldName, arrayNode);
    }

    private void putText(ObjectNode node, String fieldName, String value) {
        if (value != null) {
            node.put(fieldName, value);
        }
    }

    private void setListIfPresent(JsonNode node, String fieldName, java.util.function.Consumer<List<String>> consumer) {
        var arrayNode = node.get(fieldName);
        if (arrayNode == null || arrayNode.isNull()) {
            return;
        }
        consumer.accept(
                objectMapper.convertValue(
                        arrayNode,
                        objectMapper.getTypeFactory().constructCollectionType(List.class, String.class)
                )
        );
    }

    private void setTextIfPresent(JsonNode node, String fieldName, java.util.function.Consumer<String> consumer) {
        var valueNode = node.get(fieldName);
        if (valueNode != null && !valueNode.isNull()) {
            consumer.accept(valueNode.asText());
        }
    }
}
