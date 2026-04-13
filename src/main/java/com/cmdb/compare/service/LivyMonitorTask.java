package com.cmdb.compare.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.cmdb.compare.entity.ReconciliationTask;
import com.cmdb.compare.mapper.TaskMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class LivyMonitorTask {

    @Autowired
    private LivyService livyService;

    @Autowired
    private TaskMapper taskMapper;

    /**
     * Periodically check the status of running Livy batches.
     * Polls every 10 seconds.
     */
    @Scheduled(fixedRate = 10000)
    public void monitorLivyJobs() {
        // Query tasks that are still in RUNNING status and have a livyBatchId
        QueryWrapper<ReconciliationTask> query = new QueryWrapper<>();
        query.eq("status", "RUNNING");
        query.isNotNull("livy_batch_id");

        List<ReconciliationTask> runningTasks = taskMapper.selectList(query);
        for (ReconciliationTask task : runningTasks) {
            try {
                String state = livyService.getBatchState(task.getLivyBatchId());
                System.out.println("Polling Livy Batch ID [" + task.getLivyBatchId() + "] State: " + state);

                if ("success".equalsIgnoreCase(state)) {
                    task.setStatus("FINISHED");
                    task.setEndTime(LocalDateTime.now());
                    taskMapper.updateById(task);
                } else if ("dead".equalsIgnoreCase(state) || "error".equalsIgnoreCase(state)) {
                    task.setStatus("FAILED");
                    task.setEndTime(LocalDateTime.now());
                    task.setErrorMsg("Livy job terminated with state: " + state);
                    taskMapper.updateById(task);
                }
                // If it's starting/running, we just keep it as RUNNING
            } catch (org.springframework.web.client.HttpClientErrorException.NotFound e) {
                // Batch was deleted from Livy (e.g., after cluster restart)
                System.out.println("Livy Batch ID [" + task.getLivyBatchId() + "] no longer exists, marking task as FAILED.");
                task.setStatus("FAILED");
                task.setEndTime(LocalDateTime.now());
                task.setErrorMsg("Livy batch not found (deleted or cluster restarted)");
                taskMapper.updateById(task);
            } catch (Exception e) {
                // Potential network error during polling
                e.printStackTrace();
            }
        }
    }
}
