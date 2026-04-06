package com.cmdb.compare;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@MapperScan("com.cmdb.compare.mapper")
@EnableScheduling
@EnableAsync
public class CmdbCompareApplication {
    public static void main(String[] args) {
        SpringApplication.run(CmdbCompareApplication.class, args);
    }
}
