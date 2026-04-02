package com.cmdb.compare.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.handlers.JacksonTypeHandler;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@TableName(value = "t_reconciliation_config", autoResultMap = true)
public class ReconciliationConfig {

    @TableId
    private String configId;
    
    private String taskName;
    private String sourceFileId;
    private String rpFileId;
    private String primaryKey;
    
    @TableField(typeHandler = JacksonTypeHandler.class)
    private List<String> compareFields;
    
    @TableField(typeHandler = JacksonTypeHandler.class)
    private Map<String, String> fieldMapping;
    
    private Boolean enableFilter;
    
    @TableField(typeHandler = JacksonTypeHandler.class)
    private List<String> filterRuleIds;
    
    private String filterLogic;
    private LocalDateTime createTime;
    private String createUser;
    private LocalDateTime updateTime;
    private String status;
}
