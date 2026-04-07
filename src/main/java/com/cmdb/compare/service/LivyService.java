package com.cmdb.compare.service;

import com.cmdb.compare.config.LivyProperties;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class LivyService {

    @Autowired
    private LivyProperties livyProperties;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Submits a Spark Batch Job to Livy.
     */
    public Integer submitBatch(String mainClass, List<String> args) {
        String url = livyProperties.getUrl() + "/batches";

        Map<String, Object> request = new LinkedHashMap<>();
        request.put("file", livyProperties.getJobJar());
        request.put("className", mainClass);
        request.put("args", args);

        Map<String, String> conf = new LinkedHashMap<>();
        if (livyProperties.getSparkConf() != null) {
            livyProperties.getSparkConf().forEach((k, v) ->
                    conf.put(k, v == null ? "" : String.valueOf(v)));
        }
        if (!conf.containsKey("spark.yarn.submit.waitAppCompletion")) {
            conf.put("spark.yarn.submit.waitAppCompletion", "false");
        }
        request.put("conf", conf);

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
        String url = livyProperties.getUrl() + "/batches/" + batchId + "/state";
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
