package com.cmdb.compare.model;

import lombok.Data;

import java.util.List;

@Data
public class RuleCondition {
    private String fieldName;
    private String operator; // =, !=, <, >, <=, >=, IN, NOT_IN, IS_NULL, IS_NOT_NULL
    private Object value;
    private List<Object> values; // for IN, NOT_IN
}
