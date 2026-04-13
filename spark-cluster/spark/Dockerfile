# Build Spark image with Java 8
FROM eclipse-temurin:8-jdk-jammy

USER root

# Copy Spark runtime from the official image
COPY --from=apache/spark:3.3.1 /opt/spark /opt/spark

# Install system dependencies: tini + font engines (for Apache POI Excel generation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini fontconfig libfreetype6 \
    && rm -rf /var/lib/apt/lists/*

# Ensure scripts have execution permissions
RUN chmod +x /opt/spark/bin/* /opt/spark/sbin/*

# Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

WORKDIR /opt/spark

# Use tini as the init process
ENTRYPOINT ["/usr/bin/tini", "-s", "--"]
