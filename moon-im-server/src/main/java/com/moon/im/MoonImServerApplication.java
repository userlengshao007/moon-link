package com.moon.im;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@MapperScan("com.moon.im.mapper")
@SpringBootApplication
public class MoonImServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MoonImServerApplication.class, args);
    }
}
