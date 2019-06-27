package com.deyu.gmall.publish.dwpublish;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.deyu.gmall.publish.dwpublish.mapper")
public class DwPublishApplication {

    public static void main(String[] args) {
        SpringApplication.run(DwPublishApplication.class, args);
    }

}
