package com.cmdb.compare.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Livy REST client settings and Spark conf forwarded to POST /batches.
 */
@Data
@Component
@ConfigurationProperties(prefix = "livy")
public class LivyProperties {

    private String url = "http://localhost:8998";

    /**
     * Application jar URI visible to the Livy server (e.g. file:///data/... or s3a://...).
     */
    private String jobJar = "";

    /**
     * Keys and values are sent as Livy batch {@code conf} (all values stringified).
     */
    private Map<String, String> sparkConf = new LinkedHashMap<>();
}
