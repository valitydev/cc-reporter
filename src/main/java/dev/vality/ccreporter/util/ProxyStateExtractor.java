package dev.vality.ccreporter.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.damsel.payment_processing.SessionChangePayload;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
public class ProxyStateExtractor {

    private final ObjectMapper objectMapper;

    public String extractProviderTrxId(SessionChangePayload payload) {
        if (payload == null || !payload.isSetSessionProxyStateChanged()) {
            return null;
        }
        var proxyStateChanged = payload.getSessionProxyStateChanged();
        if (proxyStateChanged.getProxyState() == null) {
            return null;
        }
        try {
            var proxyStateJson = objectMapper.readTree(new String(
                    proxyStateChanged.getProxyState(),
                    StandardCharsets.UTF_8
            ));
            var providerTrxId = proxyStateJson.path("providerTrxId").asText(null);
            return providerTrxId != null ? providerTrxId : proxyStateJson.path("trxId").asText(null);
        } catch (IOException ex) {
            return null;
        }
    }
}
