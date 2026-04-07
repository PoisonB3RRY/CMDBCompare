package com.cmdb.compare.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.aviator.AviatorEvaluator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;

// @Service // Commented out to prevent Spark JVM initialization clashes. Logic is executed remotely in Livy.
public class RuleEngineService {

    @Autowired
    private SparkSession sparkSession;

    private static final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void registerAviatorUdf() {
        // Registers a UDF: aviator_eval(rowJson, expression) -> boolean
        sparkSession.udf().register("aviator_eval", (UDF2<String, String, Boolean>) (rowJson, expression) -> {
            if (expression == null || expression.trim().isEmpty()) {
                return true;
            }
            if (rowJson == null) {
                return false;
            }

            try {
                // parse JSON string to map
                Map<String, Object> env = mapper.readValue(rowJson, new TypeReference<Map<String, Object>>() {
                });

                // execute rule with caching enabled for high performance
                Object result = AviatorEvaluator.execute(expression, env, true);

                if (result instanceof Boolean) {
                    return (Boolean) result;
                }
                return false;
            } catch (Exception e) {
                // Return false or handle error appropriately if rule execution fails
                return false;
            }
        }, DataTypes.BooleanType);
    }
}
