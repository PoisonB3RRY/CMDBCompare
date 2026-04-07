package com.cmdb.compare.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// @Configuration // Commented out to completely eradicate local Spark Initialization from Spring Boot
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName("CMDBCompare-SpringBoot")
                .setMaster("local[*]") // Local master used merely for Spring context hydration
                .set("spark.ui.enabled", "false"); // Disable embedded Spark UI to prevent Jetty port conflicts with Tomcat

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }
}
