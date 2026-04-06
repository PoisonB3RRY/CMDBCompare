package com.cmdb.compare.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.aviator.AviatorEvaluator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Dedicated Spark Job to be submitted via Livy.
 * args: sourcePath, targetPath, pks, srcFilter, tgtFilter, compareFields,
 * outputDir, endpoint, ak, sk
 */
public class SparkCompareJob {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 10) {
            System.err.println("Usage: SparkCompareJob <src> <tgt> <pks> <srcF> <tgtF> <fields> <out> <end> <ak> <sk>");
            System.exit(1);
        }

        String sourcePath = args[0];
        String targetPath = args[1];
        List<String> pkList = Arrays.asList(args[2].split(","));
        String srcExpr = args[3];
        String tgtExpr = args[4];
        List<String> compareFields = args[5].isEmpty() ? new ArrayList<>() : Arrays.asList(args[5].split(","));
        String outputDir = args[6];
        String endpoint = args[7];
        String ak = args[8];
        String sk = args[9];

        SparkConf conf = new SparkConf().setAppName("Spark-Compare-Job-Remote");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Configure OBS
        org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.endpoint", endpoint);
        hadoopConf.set("fs.s3a.access.key", ak);
        hadoopConf.set("fs.s3a.secret.key", sk);
        hadoopConf.set("fs.s3a.path.style.access", "true");

        // Register Aviator UDF
        spark.udf().register("aviator_eval", (UDF2<String, String, Boolean>) (rowJson, expression) -> {
            if (expression == null || expression.trim().isEmpty() || "null".equals(expression))
                return true;
            if (rowJson == null)
                return false;
            Map<String, Object> env = mapper.readValue(rowJson, new TypeReference<Map<String, Object>>() {
            });
            Object result = AviatorEvaluator.execute(expression, env, true);
            return result instanceof Boolean ? (Boolean) result : false;
        }, DataTypes.BooleanType);

        // 1. Read
        Dataset<Row> sourceDf = spark.read().option("header", "true").csv(sourcePath);
        Dataset<Row> targetDf = spark.read().option("header", "true").csv(targetPath);

        // 2 & 3. Filter
        Column srcCond = (srcExpr != null && !srcExpr.trim().isEmpty() && !"null".equals(srcExpr))
                ? callUDF("aviator_eval", Anthony_toJson(sourceDf), lit(srcExpr))
                : lit(true);
        Dataset<Row> sourceDb = sourceDf.filter(srcCond);
        Dataset<Row> sourceFilteredOut = sourceDf.filter(not(srcCond));

        Column tgtCond = (tgtExpr != null && !tgtExpr.trim().isEmpty() && !"null".equals(tgtExpr))
                ? callUDF("aviator_eval", Anthony_toJson(targetDf), lit(tgtExpr))
                : lit(true);
        Dataset<Row> targetDb = targetDf.filter(tgtCond);
        Dataset<Row> targetFilteredOut = targetDf.filter(not(tgtCond));

        // 4. Compare
        if (compareFields.isEmpty()) {
            List<String> sCols = new ArrayList<>(Arrays.asList(sourceDf.columns()));
            List<String> tCols = Arrays.asList(targetDf.columns());
            sCols.retainAll(tCols);
            sCols.removeAll(pkList);
            compareFields = sCols;
        }

        Column joinCond = null;
        for (String pk : pkList) {
            Column c = col("S." + pk).equalTo(col("T." + pk));
            joinCond = (joinCond == null) ? c : joinCond.and(c);
        }

        Dataset<Row> joinedDf = sourceDb.as("S").join(targetDb.as("T"), joinCond, "outer");
        List<Column> selectCols = new ArrayList<>();
        for (String pk : pkList)
            selectCols.add(coalesce(col("S." + pk), col("T." + pk)).as(pk));

        Column hasDiff = lit(false);
        for (String f : compareFields) {
            Column s = col("S." + f);
            Column t = col("T." + f);
            Column diff = not(s.eqNullSafe(t));
            hasDiff = hasDiff.or(diff);
            selectCols.add(
                    when(diff, concat(lit("[DIFF] "), coalesce(s, lit("NULL")), lit(" -> "), coalesce(t, lit("NULL"))))
                            .otherwise(s).as(f));
        }

        Dataset<Row> diffDb = joinedDf.select(selectCols.toArray(new Column[0])).filter(hasDiff);

        // 5. Export
        export(spark, sourceFilteredOut, targetFilteredOut, diffDb, outputDir);

        spark.stop();
    }

    private static Column Anthony_toJson(Dataset<Row> df) {
        return to_json(struct(col("*")));
    }

    private static void export(SparkSession spark, Dataset<Row> sOut, Dataset<Row> tOut, Dataset<Row> diff,
            String outDir) throws Exception {
        // 创建支持流式写入的 Workbook，避免内存中积压过多导致宕机
        SXSSFWorkbook wb = new SXSSFWorkbook(100);

        // 创建差异单元格样式（高亮显示红色粗体字，黄底色）
        CellStyle diffStyle = wb.createCellStyle();
        Font font = wb.createFont();
        font.setColor(IndexedColors.RED.getIndex());
        font.setBold(true);
        diffStyle.setFont(font);
        diffStyle.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
        diffStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

        // 依据需求输出至不同Sheet
        writeSheet(wb, "Sheet1_Source_Filtered", sOut, null);
        writeSheet(wb, "Sheet2_Target_Filtered", tOut, null);
        writeSheet(wb, "Sheet3_Differences", diff, diffStyle);

        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String pathStr = (outDir.endsWith("/") ? outDir : outDir + "/") + "Compare_Result_" + ts + ".xlsx";

        org.apache.hadoop.conf.Configuration conf = spark.sparkContext().hadoopConfiguration();
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathStr);
        org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);

        // 通过文件系统将 Excel 写入至目的端 (支持 local、hdfs、s3 等)
        try (OutputStream out = fs.create(path, true)) {
            wb.write(out);
        } finally {
            wb.dispose(); // 删除 SXSSFWorkbook 产生的临时文件
            wb.close();
        }
        System.out.println("Result saved to: " + pathStr);
    }

    private static void writeSheet(SXSSFWorkbook wb, String name, Dataset<Row> df, CellStyle ds) {
        Sheet sheet = wb.createSheet(name);
        String[] cols = df.columns();
        org.apache.poi.ss.usermodel.Row head = sheet.createRow(0);

        // 表头加粗样式
        CellStyle hs = wb.createCellStyle();
        Font hf = wb.createFont();
        hf.setBold(true);
        hs.setFont(hf);

        for (int i = 0; i < cols.length; i++) {
            Cell c = head.createCell(i);
            c.setCellValue(cols[i]);
            c.setCellStyle(hs);
        }

        // 迭代处理数据回写到 Excel 中
        Iterator<Row> it = df.toLocalIterator();
        int idx = 1;
        while (it.hasNext()) {
            Row r = it.next();
            org.apache.poi.ss.usermodel.Row pr = sheet.createRow(idx++);
            for (int i = 0; i < cols.length; i++) {
                Cell c = pr.createCell(i);
                Object v = r.get(i);
                if (v != null) {
                    String s = v.toString();
                    c.setCellValue(s);
                    // 只有带有 [DIFF] 标记的才着色
                    if (ds != null && s.startsWith("[DIFF]")) {
                        c.setCellStyle(ds);
                    }
                }
            }
        }
    }
}
