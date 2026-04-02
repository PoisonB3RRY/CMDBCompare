package com.cmdb.compare.service;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class LivyService {

    @Value("${livy.url}")
    private String livyUrl;

    @Value("${livy.job-jar}")
    private String jobJar;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Submits a Spark Batch Job to Livy.
     */
    public Integer submitBatch(String mainClass, List<String> args) {
        String url = livyUrl + "/batches";

        Map<String, Object> request = new HashMap<>();
        request.put("file", jobJar);
        request.put("className", mainClass);
        request.put("args", args);
        // Optional: conf, driverMemory, executorMemory etc.
        request.put("conf", Collections.singletonMap("spark.yarn.submit.waitAppCompletion", "false"));

        Map<String, Object> response = restTemplate.postForObject(url, request, Map.class);
        if (response != null && response.containsKey("id")) {
            return (Integer) response.get("id");
        }
        throw new RuntimeException("Failed to submit Livy batch: " + response);
    }

    /**
     * Gets the state of a Livy batch.
     */
    public String getBatchState(int batchId) {
        String url = livyUrl + "/batches/" + batchId + "/state";
        Map<String, Object> response = restTemplate.getForObject(url, Map.class);
        if (response != null && response.containsKey("state")) {
            return (String) response.get("state");
        }
        return "unknown";
    }

    @Data
    public static class LivyBatchResponse {
        private int id;
        private String state;
    }
}
