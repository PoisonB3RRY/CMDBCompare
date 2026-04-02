package com.cmdb.compare.model;

import lombok.Data;

import java.util.List;

@Data
public class CompareRequest {
    private String sourceFilePath;
    private String targetFilePath;
    
    // the keys used for joining two datasets
    private List<String> primaryKeys;
    
    // Valid Aviator expressions e.g. "age > 30 && status == 'ACTIVE'"
    private String sourceFilterExpression;
    private String targetFilterExpression;
    
    // fields to compare. if empty, compare all matching fields minus primary keys
    private List<String> compareFields;
    
    // Directory to save final Excel: e.g. obs://bucket/results/
    private String outputDirPath;
}
