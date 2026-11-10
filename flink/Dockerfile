# Image Flink enrichie : Python + PyFlink + connecteur Kafka.
FROM flink:1.18.1-scala_2.12-java11

# Python + outils de build. PyFlink depend de `pemja`, qui se compile depuis les
# sources (pas de wheel ARM) et exige les EN-TETES Java : l'image Flink n'ayant
# qu'un JRE, on installe un JDK et on expose ses headers sous JAVA_HOME.
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev build-essential curl \
        openjdk-11-jdk-headless && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    JDK_INC="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")/include" && \
    ln -sfn "$JDK_INC" /opt/java/openjdk/include && \
    rm -rf /var/lib/apt/lists/*

# PyFlink, version alignée sur le cluster.
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Connecteur Kafka (DataStream API) posé sur le classpath du cluster.
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
