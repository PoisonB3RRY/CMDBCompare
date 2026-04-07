package com.cmdb.compare.service;

import com.cmdb.compare.mapper.TaskMapper;
import com.cmdb.compare.model.CompareRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CompareServiceTest {

    @Mock
    private LivyService livyService;

    @Mock
    private TaskMapper taskMapper;

    @InjectMocks
    private CompareService compareService;

    @Test
    void performCompare_submitsSparkJobAndReturnsTaskId() {
        ReflectionTestUtils.setField(compareService, "obsEndpoint", "http://obs");
        ReflectionTestUtils.setField(compareService, "obsAccessKey", "ak");
        ReflectionTestUtils.setField(compareService, "obsSecretKey", "sk");

        when(livyService.submitBatch(any(), any())).thenReturn(7);
        when(taskMapper.insert(any())).thenReturn(1);
        when(taskMapper.updateById(any())).thenReturn(1);

        CompareRequest request = new CompareRequest();
        request.setSourceFilePath("file:///data/source.csv");
        request.setTargetFilePath("file:///data/target.csv");
        request.setPrimaryKeys(Collections.singletonList("id"));
        request.setSourceFilterExpression(null);
        request.setTargetFilterExpression(null);
        request.setCompareFields(Collections.emptyList());
        request.setOutputDirPath("file:///data/out/");

        String taskId = compareService.performCompare(request);

        assertNotNull(taskId);

        ArgumentCaptor<List<String>> argsCap = ArgumentCaptor.forClass(List.class);
        verify(livyService).submitBatch(
                org.mockito.ArgumentMatchers.eq("com.cmdb.compare.job.SparkCompareJob"),
                argsCap.capture());
        List<String> args = argsCap.getValue();
        assertEquals("file:///data/source.csv", args.get(0));
        assertEquals("file:///data/target.csv", args.get(1));
        assertEquals("id", args.get(2));
        assertEquals(Arrays.asList("id", "null", "null", "", "file:///data/out/", "http://obs", "ak", "sk"),
                args.subList(2, 10));
    }
}
