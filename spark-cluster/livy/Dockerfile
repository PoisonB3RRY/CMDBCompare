# Stage 1: 解压 Livy
FROM eclipse-temurin:11-jdk-jammy AS livy-unpack

ARG LIVY_VERSION=0.8.0-incubating
ARG LIVY_SCALA_TAG=2.12

ADD https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_${LIVY_SCALA_TAG}-bin.zip /tmp/livy.zip

WORKDIR /opt
RUN jar xf /tmp/livy.zip \
    && mv "apache-livy-${LIVY_VERSION}_${LIVY_SCALA_TAG}-bin" livy \
    && rm -f /tmp/livy.zip

RUN mkdir -p /opt/livy/logs

# Stage 2: 运行环境 — 使用 Java 8 避免 Spark 3.3 + Java 11 的 JMX MBean 兼容性崩溃
# 问题: Java 11 容器中 java.nio:type=BufferPool,name=direct MBean 未注册，
#       导致 Spark 3.3.x ExecutorMetricType 静态初始化时 ExceptionInInitializerError。
# 解决: Spark 3.3 JAR 完全兼容 Java 8，使用 Java 8 不存在模块化 JMX 问题。
FROM eclipse-temurin:8-jdk-jammy

USER root

# 从官方 Spark 镜像复制 Spark 运行时
COPY --from=apache/spark:3.3.1 /opt/spark /opt/spark

# 复制 Livy
COPY --from=livy-unpack /opt/livy /opt/livy
COPY livy.conf /opt/livy/conf/livy.conf

# 安装系统依赖: tini(init进程) + 字体引擎(Apache POI Excel 生成需要)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini fontconfig libfreetype6 \
    && rm -rf /var/lib/apt/lists/*

# 确保脚本具有执行权限
RUN chmod +x /opt/livy/bin/* /opt/spark/bin/* /opt/spark/sbin/*

WORKDIR /opt/livy

ENV SPARK_HOME=/opt/spark
ENV LIVY_HOME=/opt/livy
ENV PATH="${LIVY_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

EXPOSE 8998

ENTRYPOINT ["/usr/bin/tini", "-s", "--"]
CMD ["/opt/livy/bin/livy-server"]