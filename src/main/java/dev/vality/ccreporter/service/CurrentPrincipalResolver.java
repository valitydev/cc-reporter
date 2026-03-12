package dev.vality.ccreporter.service;

import dev.vality.ccreporter.InvalidRequest;
import dev.vality.ccreporter.config.CcrApiProperties;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class CurrentPrincipalResolver {

    private final ObjectProvider<HttpServletRequest> requestProvider;
    private final CcrApiProperties apiProperties;

    public CurrentPrincipalResolver(
            ObjectProvider<HttpServletRequest> requestProvider,
            CcrApiProperties apiProperties
    ) {
        this.requestProvider = requestProvider;
        this.apiProperties = apiProperties;
    }

    public String resolveRequired() throws InvalidRequest {
        HttpServletRequest request = requestProvider.getIfAvailable();
        if (request == null) {
            throw invalidRequest("Missing HTTP request context");
        }
        String createdBy = request.getHeader(apiProperties.getCreatedByHeader());
        if (!StringUtils.hasText(createdBy)) {
            throw invalidRequest("Missing caller header: " + apiProperties.getCreatedByHeader());
        }
        return createdBy.trim();
    }

    private InvalidRequest invalidRequest(String error) {
        return new InvalidRequest(List.of(error));
    }
}
