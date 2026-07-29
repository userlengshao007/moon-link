package com.moon.im;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * IM 业务服务启动入口。
 */
@EnableKafka
@MapperScan("com.moon.im.mapper")
@SpringBootApplication
public class MoonImServerApplication {
    /**
     * 启动 Spring Boot 应用。
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        SpringApplication.run(MoonImServerApplication.class, args);
    }
}
