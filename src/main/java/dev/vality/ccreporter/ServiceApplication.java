package dev.vality.ccreporter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ServiceApplication {

    static void main(String[] args) {
        SpringApplication.run(ServiceApplication.class, args);
    }

}
