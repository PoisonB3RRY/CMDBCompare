# CMDB 高性能数据比对服务 (Spark-Livy Edition)

本项目是一个基于 **Spring Boot 2.7**、**Apache Spark 3.3**、**Livy** 及 **Aviator** 规则引擎构建的企业级文件比对系统。旨在解决百万级乃至千万级配置项（CMDB）数据的规则过滤、差异发现及 OBS 云端报表生成需求。

---

## 1. 核心技术栈
- **计算引擎**: Apache Spark 3.3 (分布式并行计算)
- **调度层**: Apache Livy (远程异步任务提交)
- **规则引擎**: AviatorScript (高性能动态表达式 UDF)
- **持久化层**: MySQL 8.0 + MyBatis-Plus (任务流水与配置管理)
- **云原生存储**: OBS/S3A (直接读写对象存储)
- **报表导出**: Apache POI (SXSSF 流式写入，防 OOM)

---

## 2. 工程目录结构
```text
CMDBComparision
├── src/main/java/com/cmdb/compare
│   ├── CmdbCompareApplication.java # 入口类，启用定期任务与异步调度
│   ├── controller
│   │   └── CompareController.java # 对外 REST API（运行比对、状态查询）
│   ├── service
│   │   ├── CompareService.java     # 调度中心：创建 DB 任务并提交 Livy
│   │   ├── LivyService.java        # Livy REST 客户端实现
│   │   ├── LivyMonitorTask.java    # 定时轮询器：同步远程 Livy 任务状态到数据库
│   │   └── RuleEngineService.java  # 表达式处理逻辑（已整合入 SparkJob）
│   ├── job
│   │   └── SparkCompareJob.java    # 远程执行核心：包含 Spark 算子、Aviator UDF、Excel 生成
│   ├── entity                      # MyBatis-Plus 实体类 (FilterRule, Task, Config)
│   ├── mapper                      # 数据库访问接口
│   └── model                       # 数据传输对象 (CompareRequest)
├── src/main/resources
│   ├── application.yml             # 数据库、OBS、Livy 集群配置
│   ├── schema.sql                  # MySQL 初始化建表脚本
│   └── mapper/                     # MyBatis XML 映射文件
└── pom.xml                         # Maven 依赖管理（含双 JAR 打包配置）
```

---

## 3. 核心设计思路与处理流程

### 3.1 总体架构思路
采用 **“计算与调度分离”** 的架构。Spring Boot 仅负责业务逻辑和任务状态维护，实际的重型 Spark 计算通过 Livy 提交到远程集群运行，确保应用本身的高可用与轻量化。

### 3.2 处理流程 (Workflow)
1. **API 接收**: 客户端通过 `/api/compare/run` 提交源/目标 OBS 路径及 Aviator 过滤规则。
2. **任务初始化**: `CompareService` 在 MySQL 生成一条 UUID 任务流水，状态置为 `RUNNING`。
3. **Livy 提交**: 系统调用 Livy 接口，将预先上传至 OBS 的 `SparkCompareJob.jar` 及其运行参数发送至集群。
4. **分布式计算 (Spark Job)**:
   - Spark 加载 OBS 数据。
   - 注册 **Aviator UDF**，利用分布式节点执行字段过滤。
   - 执行 **Full Outer Join**，并对比指定字段，使用内置逻辑标记差异项（`[DIFF] val1 -> val2`）。
   - 将差异明细与过滤排除项流式写入 **Excel**，直接上传至目标 OBS 路径。
5. **状态轮询**: `LivyMonitorTask` 定时（10s）检查 Livy 返回的状态。
6. **任务归档**: 当 Livy 任务完成或失败，更新数据库状态、结束时间及 OBS 结果链接。

---

## 4. 关键问题处理方案

| 问题场景 | 处理方法 | 所在类/逻辑 |
| :--- | :--- | :--- |
| **大数据量导出 OOM** | 使用 POI 的 `SXSSFWorkbook` 实现流式写入。数据达到一定行数（100行）即刷入临时磁盘，内存仅保留滑窗数据。 | `SparkCompareJob.export()` |
| **动态复杂规则过滤** | 集成 **Aviator**。将脚本注入 Spark UDF，在集群节点上并行执行复杂的逻辑判断（支持多条件组合、正则、逻辑运算）。 | `SparkCompareJob` 内部 UDF 注册 |
| **同名类冲突** | Spark 的 `Row` 与 POI 的 `Row` 类名冲突。在导出逻辑中使用**全限定名** `org.apache.poi.ss.usermodel.Row` 进行硬隔离。 | `SparkCompareJob.writeSheet()` |
| **远程计算隔离** | 通过 `spring-boot-maven-plugin` 配置 `classifier: exec`。生成两个 Jar：一个 Fat Jar 用于应用启动，一个标准 Jar (Thin Jar) 供 Livy/Spark 运行，解决 Spring 代码无法直接在 Spark 运行的问题。 | `pom.xml` |
| **跨地域存储读写** | 使用 Hadoop `S3A` 实现类配置。通过 Spark Hadoop Configuration 注入 OBS 的特有 Endpoint 和密钥，实现计算引擎直读直写云存储。 | `SparkCompareJob` 初始化配置 |

---

## 5. 快速开始与部署

### 5.1 数据库初始化
执行 `src/main/resources/schema.sql` 完成任务表和配置表的创建。

### 5.2 配置文件修改
编辑 `application.yml`：
- 修改 `spring.datasource` 下的连接信息。
- 修改 `obs` 部分的密钥与 endpoint。
- 修改 `livy.url` 为您真实的 Livy 控制台地址。

### 5.3 打包与上传
1. 执行 `mvn clean package`。
2. 将 `target/cmdb-compare-service-0.0.1-SNAPSHOT.jar` 上传至 OBS。
3. 将该 OBS 路径填入 `application.yml` 的 `livy.job-jar` 中。

### 5.4 提交任务示例
```json
POST /api/compare/run
{
  "sourceFilePath": "obs://bucket/source.csv",
  "targetFilePath": "obs://bucket/target.csv",
  "primaryKeys": ["id"],
  "sourceFilterExpression": "status == 'ACTIVE' && age > 18",
  "targetFilterExpression": "status == 'ACTIVE'",
  "outputDirPath": "obs://bucket/results/"
}
```
返回：`{"taskId": "xxxx-xxxx", "status": "SUBMITTED"}`。
之后调用 `GET /api/compare/status/{taskId}` 获取进度。
