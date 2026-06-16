package com.moon.im;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class MoonImServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MoonImServerApplication.class, args);
    }
}
