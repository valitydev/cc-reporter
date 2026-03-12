package dev.vality.ccreporter.config;

import dev.vality.ccreporter.ReportingSrv;
import dev.vality.woody.thrift.impl.http.THServiceBuilder;
import jakarta.servlet.GenericServlet;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import java.io.IOException;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ThriftEndpointConfig {

    @Bean
    public ServletRegistrationBean<GenericServlet> reportingServlet(
            ReportingSrv.Iface requestHandler,
            CcrApiProperties apiProperties
    ) {
        ServletRegistrationBean<GenericServlet> registrationBean =
                new ServletRegistrationBean<>(new ReportingServlet(requestHandler), apiProperties.getPath());
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
