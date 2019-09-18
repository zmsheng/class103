package com.leyou;

import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ESApplication {

    public static void main(String[] args) {
        SpringApplication.run(ESApplication.class,args);
    }
}
