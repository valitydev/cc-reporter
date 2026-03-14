package dev.vality.ccreporter.security;

import dev.vality.ccreporter.InvalidRequest;
import dev.vality.ccreporter.config.properties.CcrApiProperties;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

@Component
@RequiredArgsConstructor
public class CurrentPrincipalResolver {

    private final ObjectProvider<HttpServletRequest> requestProvider;
    private final CcrApiProperties apiProperties;

    public String resolveRequired() throws InvalidRequest {
        var request = requestProvider.getIfAvailable();
        if (request == null) {
            throw invalidRequest("Missing HTTP request context");
        }
        var createdBy = request.getHeader(apiProperties.getCreatedByHeader());
        if (!StringUtils.hasText(createdBy)) {
            throw invalidRequest("Missing caller header: " + apiProperties.getCreatedByHeader());
        }
        return createdBy.trim();
    }

    private InvalidRequest invalidRequest(String error) {
        return new InvalidRequest(List.of(error));
    }
}
