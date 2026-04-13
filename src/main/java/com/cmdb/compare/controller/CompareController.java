package com.cmdb.compare.controller;

import com.cmdb.compare.entity.ReconciliationTask;
import com.cmdb.compare.mapper.TaskMapper;
import com.cmdb.compare.model.CompareRequest;
import com.cmdb.compare.service.CompareService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/compare")
public class CompareController {

    @Autowired
    private CompareService compareService;

    @Autowired
    private TaskMapper taskMapper;

    /**
     * Triggers a remote Spark comparison via Livy.
     * Returns a JSON with the taskId.
     */
    @PostMapping("/run")
    public ResponseEntity<Map<String, String>> runCompare(@RequestBody CompareRequest request) {
        try {
            String taskId = compareService.performCompare(request);
            Map<String, String> response = new HashMap<>();
            response.put("taskId", taskId);
            response.put("status", "SUBMITTED");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> err = new HashMap<>();
            err.put("error", e.getClass().getName() + ": " + e.getMessage());
            return ResponseEntity.internalServerError().body(err);
        }
    }

    /**
     * Polls the status of a specific task.
     */
    @GetMapping("/status/{taskId}")
    public ResponseEntity<ReconciliationTask> getStatus(@PathVariable String taskId) {
        ReconciliationTask task = taskMapper.selectById(taskId);
        if (task == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(task);
    }

    /**
     * Downloads the comparison result Excel file for a specific task.
     * Extracts the filename from the task's resultPath and serves it
     * from the local spark-cluster/data/results/ directory.
     */
    @GetMapping("/download/{taskId}")
    public ResponseEntity<Resource> downloadResult(@PathVariable String taskId) {
        ReconciliationTask task = taskMapper.selectById(taskId);
        if (task == null || task.getResultPath() == null || task.getResultPath().isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        // resultPath is like "file:///data/results/Compare_Result_xxx.xlsx"
        // Extract just the filename
        String resultPath = task.getResultPath();
        String fileName = resultPath.substring(resultPath.lastIndexOf('/') + 1);

        try {
            // The Docker volume maps ./spark-cluster/data to /data inside the container
            Path file = Paths.get("./spark-cluster/data/results").resolve(fileName);
            Resource resource = new UrlResource(file.toUri());
            if (resource.exists() && resource.isReadable()) {
                return ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                        .body(resource);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.notFound().build();
    }
}
