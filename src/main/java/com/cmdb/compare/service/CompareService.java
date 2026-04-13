package com.cmdb.compare.service;

import com.cmdb.compare.entity.ReconciliationTask;
import com.cmdb.compare.mapper.TaskMapper;
import com.cmdb.compare.model.CompareRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class CompareService {

    @Autowired
    private LivyService livyService;

    @Autowired
    private TaskMapper taskMapper;

    @Value("${obs.endpoint}")
    private String obsEndpoint;

    @Value("${obs.access-key}")
    private String obsAccessKey;

    @Value("${obs.secret-key}")
    private String obsSecretKey;

    /**
     * Orchestrates the comparison by submitting a batch job to remote Livy.
     * Returns the taskId for polling.
     */
    public String performCompare(CompareRequest request) {
        String taskId = UUID.randomUUID().toString();
        
        // 1. Initialize Task in DB
        ReconciliationTask task = new ReconciliationTask();
        task.setTaskId(taskId);
        task.setTaskName("CompareTask_" + LocalDateTime.now());
        task.setStartTime(LocalDateTime.now());
        task.setStatus("RUNNING");
        task.setCreateTime(LocalDateTime.now());
        taskMapper.insert(task);

        // 2. Prepare Arguments for SparkCompareJob
        // Usage: SparkCompareJob <src> <tgt> <pks> <srcF> <tgtF> <fields> <out> <end> <ak> <sk>
        List<String> args = new ArrayList<>();
        args.add(request.getSourceFilePath());
        args.add(request.getTargetFilePath());
        args.add(String.join(",", request.getPrimaryKeys()));
        args.add(request.getSourceFilterExpression() != null ? request.getSourceFilterExpression() : "null");
        args.add(request.getTargetFilterExpression() != null ? request.getTargetFilterExpression() : "null");
        args.add(request.getCompareFields() != null ? String.join(",", request.getCompareFields()) : "");
        
        String outputDir = request.getOutputDirPath();
        if (outputDir == null || outputDir.isEmpty()) {
            outputDir = "file:///data/results/";
        }

        // Generate a deterministic full path so we can save it in DB now
        String fileName = "Compare_Result_" + taskId + ".xlsx";
        String fullResultPath = outputDir.endsWith("/") ? outputDir + fileName : outputDir + "/" + fileName;
        
        task.setResultPath(fullResultPath);
        args.add(fullResultPath);
        
        args.add(obsEndpoint);
        args.add(obsAccessKey);
        args.add(obsSecretKey);

        // 3. Submit to Livy
        Integer batchId = livyService.submitBatch("com.cmdb.compare.job.SparkCompareJob", args);
        
        // 4. Update Task with BatchId
        task.setLivyBatchId(batchId);
        taskMapper.updateById(task);

        return taskId;
    }
}
