package com.cmdb.compare.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.handlers.JacksonTypeHandler;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName(value = "t_filter_rule", autoResultMap = true)
public class FilterRule {

    @TableId
    private String ruleId;
    
    private String ruleName;
    private String ruleDescription;
    private String ruleDimension;
    private String ruleCategory;
    private String ruleType;
    
    // JSON mapping
    @TableField(typeHandler = JacksonTypeHandler.class)
    private Object ruleDefinition;
    
    private Boolean enabled;
    private LocalDateTime createTime;
    private String createUser;
    private Integer sortOrder;
}
