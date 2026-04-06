package com.cmdb.compare.controller;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/files")
public class FileUploadController {

    private final String DATA_DIR = "./spark-cluster/data";

    @PostMapping("/upload")
    public ResponseEntity<Map<String, String>> uploadFile(@RequestParam("file") MultipartFile file) {
        Map<String, String> result = new HashMap<>();
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(result);
        }
        try {
            Path root = Paths.get(DATA_DIR);
            if (!Files.exists(root)) {
                Files.createDirectories(root);
            }
            String originalName = file.getOriginalFilename();
            String extension = originalName != null && originalName.contains(".") ? originalName.substring(originalName.lastIndexOf(".")) : ".csv";
            String newFileName = UUID.randomUUID().toString() + extension;
            Path dest = root.resolve(newFileName).normalize().toAbsolutePath();
            file.transferTo(dest.toFile());
            
            // Generate path understandable by Spark docker containers
            result.put("path", "file:///data/" + newFileName);
            result.put("fileName", originalName);
            return ResponseEntity.ok(result);
        } catch (IOException e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/latest-result")
    public ResponseEntity<Map<String, String>> getLatestResult() {
        try {
            Path resultsDir = Paths.get(DATA_DIR, "results");
            if (!Files.exists(resultsDir)) return ResponseEntity.notFound().build();
            // Default Find most recent Excel spreadsheet output
            Optional<Path> latestFile = Files.list(resultsDir)
                    .filter(p -> p.toString().endsWith(".xlsx"))
                    .max((p1, p2) -> Long.compare(p1.toFile().lastModified(), p2.toFile().lastModified()));
            
            if (latestFile.isPresent()) {
                Map<String, String> res = new HashMap<>();
                res.put("fileName", latestFile.get().getFileName().toString());
                return ResponseEntity.ok(res);
            }
        } catch (Exception e) {}
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/download")
    public ResponseEntity<Resource> downloadResult(@RequestParam("fileName") String fileName) {
        try {
            Path file = Paths.get(DATA_DIR, "results").resolve(fileName);
            Resource resource = new UrlResource(file.toUri());
            if (resource.exists() || resource.isReadable()) {
                return ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + resource.getFilename() + "\"")
                        .body(resource);
            }
        } catch (Exception e) {}
        return ResponseEntity.notFound().build();
    }
}
