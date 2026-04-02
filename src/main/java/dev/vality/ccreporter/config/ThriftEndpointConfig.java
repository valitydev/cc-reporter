package dev.vality.ccreporter.config;

import dev.vality.ccreporter.ReportingSrv;
import dev.vality.ccreporter.config.properties.CcrApiProperties;
import dev.vality.woody.thrift.impl.http.THServiceBuilder;
import jakarta.servlet.*;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class ThriftEndpointConfig {

    @Bean
    public ServletRegistrationBean<GenericServlet> reportingServlet(
            ReportingSrv.Iface requestHandler,
            CcrApiProperties apiProperties
    ) {
        var registrationBean =
                new ServletRegistrationBean<GenericServlet>(
                        new ReportingServlet(requestHandler),
                        apiProperties.getPath()
                );
        registrationBean.setName("ccrReportingThriftServlet");
        registrationBean.setLoadOnStartup(1);
        return registrationBean;
    }

    private static final class ReportingServlet extends GenericServlet {

        private final ReportingSrv.Iface requestHandler;
        private Servlet thriftServlet;

        private ReportingServlet(ReportingSrv.Iface requestHandler) {
            this.requestHandler = requestHandler;
        }

        @Override
        public void init(ServletConfig config) throws ServletException {
            super.init(config);
            thriftServlet = new THServiceBuilder().build(ReportingSrv.Iface.class, requestHandler);
        }

        @Override
        public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
            thriftServlet.service(req, res);
        }
    }
}
