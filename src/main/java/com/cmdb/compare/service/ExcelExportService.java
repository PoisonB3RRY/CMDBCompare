package com.cmdb.compare.service;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

@Service
public class ExcelExportService {

    @Autowired
    private SparkSession sparkSession;

    /**
     * Exports three distinct datasets into three sheets.
     */
    public String exportDiffToExcel(Dataset<Row> sourceFilteredOut,
                                    Dataset<Row> targetFilteredOut,
                                    Dataset<Row> diffDb,
                                    String outputDir) throws IOException {

        // SXSSFWorkbook configuration for streaming to avoid OOM
        SXSSFWorkbook workbook = new SXSSFWorkbook(100); 
        
        CellStyle diffStyle = createDiffStyle(workbook);

        writeSheet(workbook, "Source_Filtered_Out", sourceFilteredOut, null);
        writeSheet(workbook, "Target_Filtered_Out", targetFilteredOut, null);
        // Diff DB has special styling if columns end with '_diff' -> meaning it differs
        writeSheet(workbook, "Differences", diffDb, diffStyle);

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        // ensure trailing slash
        if (!outputDir.endsWith("/")) outputDir += "/";
        String filePath = outputDir + "Compare_Result_" + timestamp + ".xlsx";

        boolean isCloud = filePath.startsWith("obs://") || filePath.startsWith("s3a://") || filePath.startsWith("s3://");

        try {
            if (isCloud) {
                org.apache.hadoop.conf.Configuration conf = sparkSession.sparkContext().hadoopConfiguration();
                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);
                org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
                try (java.io.OutputStream out = fs.create(path, true)) {
                    workbook.write(out);
                }
            } else {
                try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath)) {
                    workbook.write(fos);
                }
            }
        } finally {
            workbook.dispose(); // delete temp files
            workbook.close();
        }

        return filePath;
    }

    private void writeSheet(SXSSFWorkbook workbook, String sheetName, Dataset<Row> df, CellStyle diffStyle) {
        Sheet sheet = workbook.createSheet(sheetName);
        
        if (df == null) return;
        
        String[] columns = df.columns();
        
        // Header
        org.apache.poi.ss.usermodel.Row headerRow = sheet.createRow(0);
        for (int i = 0; i < columns.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(columns[i]);
            // bold header
            CellStyle headerStyle = workbook.createCellStyle();
            Font font = workbook.createFont();
            font.setBold(true);
            headerStyle.setFont(font);
            cell.setCellStyle(headerStyle);
        }

        // Data Rows
        Iterator<Row> iterator = df.toLocalIterator();
        int rowIdx = 1;
        while (iterator.hasNext()) {
            Row sparkRow = iterator.next();
            org.apache.poi.ss.usermodel.Row poiRow = sheet.createRow(rowIdx++);
            
            for (int i = 0; i < columns.length; i++) {
                Cell cell = poiRow.createCell(i);
                Object val = sparkRow.get(i);
                if (val != null) {
                    cell.setCellValue(val.toString());
                }

                // If this is the Differences sheet, check if value indicates difference
                // Assume logic in CompareService passes string like "[DIFF] val1 -> val2"
                if (diffStyle != null && val != null && val.toString().startsWith("[DIFF]")) {
                    cell.setCellStyle(diffStyle);
                }
            }
        }
    }

    private CellStyle createDiffStyle(SXSSFWorkbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setColor(IndexedColors.RED.getIndex());
        font.setBold(true);
        style.setFont(font);
        
        style.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return style;
    }
}
