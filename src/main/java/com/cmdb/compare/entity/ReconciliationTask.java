package com.cmdb.compare.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("t_reconciliation_task")
public class ReconciliationTask {

    @TableId
    private String taskId;
    
    private String taskName;
    private String configId;
    
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long duration;
    
    private String status;
    private String errorMsg;
    
    private Long sourceRecordCount;
    private Long rpRecordCountBefore;
    private Long rpRecordCountAfter;
    
    private Long diff1Count;
    private Long diff2Count;
    private Long diff3Count;
    private Long diff4Count;
    
    private String resultPath;
    private Integer livyBatchId;
    private LocalDateTime createTime;
}
