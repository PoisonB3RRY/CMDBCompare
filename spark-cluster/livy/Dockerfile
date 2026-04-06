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

# Stage 2: 运行环境
FROM apache/spark:3.3.1

# 切换到 root 以执行安装和配置
USER root

COPY --from=livy-unpack /opt/livy /opt/livy
COPY livy.conf /opt/livy/conf/livy.conf

# 安装 Apache POI Excel 生成所需的系统字体引擎依赖
RUN apt-get update && apt-get install -y fontconfig libfreetype6 && rm -rf /var/lib/apt/lists/*

# 核心修改：确保 bin 目录下的脚本具有执行权限
RUN chmod +x /opt/livy/bin/*

# 移除 USER 185，或者显式声明使用 root
USER root

WORKDIR /opt/livy

ENV LIVY_HOME=/opt/livy
ENV PATH="${LIVY_HOME}/bin:${PATH}"

EXPOSE 8998

# 建议使用绝对路径
CMD ["/opt/livy/bin/livy-server"]