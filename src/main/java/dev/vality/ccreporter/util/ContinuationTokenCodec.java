package dev.vality.ccreporter.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.BadContinuationToken;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

@Component
public class ContinuationTokenCodec {

    private final ObjectMapper objectMapper;

    public ContinuationTokenCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String encode(Instant createdAt, long reportId) {
        try {
            var node = objectMapper.createObjectNode();
            node.put("created_at", createdAt.toString());
            node.put("report_id", reportId);
            var payload = objectMapper.writeValueAsBytes(node);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(payload);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to encode continuation token", ex);
        }
    }

    public PageCursor decode(String token) throws BadContinuationToken {
        try {
            var decoded = Base64.getUrlDecoder().decode(token.getBytes(StandardCharsets.UTF_8));
            var rootNode = objectMapper.readTree(decoded);
            return new PageCursor(
                    Instant.parse(rootNode.get("created_at").asText()),
                    rootNode.get("report_id").asLong()
            );
        } catch (Exception ex) {
            var badContinuationToken = new BadContinuationToken();
            badContinuationToken.setReason("Malformed continuation token");
            throw badContinuationToken;
        }
    }

    public record PageCursor(Instant createdAt, long reportId) {
    }
}
