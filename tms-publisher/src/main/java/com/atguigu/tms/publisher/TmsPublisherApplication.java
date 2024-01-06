package com.atguigu.tms.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.tms.publisher.mapper")
public class TmsPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(TmsPublisherApplication.class, args);
    }

}
