package com.cmdb.compare.service;

import com.cmdb.compare.model.CompareRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

@SpringBootTest
public class CompareServiceTest {

    @Autowired
    private CompareService compareService;

    @Test
    public void testPerformCompare() throws IOException {
        CompareRequest request = new CompareRequest();
        
        // Use absolute path for test resources
        String sourceFilePath = new File("src/test/resources/source.csv").getAbsolutePath();
        String targetFilePath = new File("src/test/resources/target.csv").getAbsolutePath();
        
        request.setSourceFilePath(sourceFilePath);
        request.setTargetFilePath(targetFilePath);
        
        request.setPrimaryKeys(Collections.singletonList("id"));
        
        // Filter out INACTIVE records using Aviator expression
        String aviatorExpr = "status == 'ACTIVE'";
        request.setSourceFilterExpression(aviatorExpr);
        request.setTargetFilterExpression(aviatorExpr);
        
        String resultPath = compareService.performCompare(request);
        
        Assertions.assertNotNull(resultPath);
        File resultFile = new File(resultPath);
        Assertions.assertTrue(resultFile.exists(), "Excel result file should exist");
        
        System.out.println("Test successfully generated excel at: " + resultPath);
    }
}
