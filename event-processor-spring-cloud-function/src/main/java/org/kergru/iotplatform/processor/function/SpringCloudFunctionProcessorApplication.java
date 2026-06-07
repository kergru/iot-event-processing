package org.kergru.iotplatform.processor.function;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SpringCloudFunctionProcessorApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringCloudFunctionProcessorApplication.class, args);
  }
}
