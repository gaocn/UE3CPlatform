#!/usr/bin/env bash
# 一次作业要480G 240 Core资源
/usr/local/spark/bin/spark-submit \
    --class com.govind.PageOneStepConvertRateAnalysis  \
    --num-executors 80            \ #一般是50~100
    --driver-memory 6g            \ #一般1~5g
    --executor-memory 6g          \ #一般6~10g
    --executor-cores 3            \
    --master yarn-cluster         \
    --queue root.default          \
    --conf spark.yarn.executor.memoryOverhead=2048 \
    --conf spark.core.connection.ack.wait.timeout=300 \
    # 生产上运行读取Hive数据需要以下配置
    # 1、访问hive数据源需要指定Hive配置和MySQL connector
    # 2、将hive-site.xml文件拷贝到Spark的conf目录下否则Spark找不到Hive元数据
    # 3、注意hive表再hdfs的访问权限
    --files /usr/local/hive/conf/hive-site.xml \
    --driver-class-path /usr/local/hive/lib/mysql-connector-java-5.1.17.jar \
    /usr/local/spark/spark-with-dependencies.jar \
    ${1}

