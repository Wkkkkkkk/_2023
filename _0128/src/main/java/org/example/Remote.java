package org.example;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * @author wuke
 * @date 2023-01-28
 **/

public class Remote {
    public static void main(String[] args) throws MalformedURLException {
        List<String> jarlist = new ArrayList<>();

        List<String> versionPath = new ArrayList<>();
        String path = "/Users/wuke/Documents/baoxin/xinsight_conf/sts/lib/flink/1.16.0";
        if (FileUtil.isDirectory(path)) {
            versionPath = getFileList(path);
        } else {
        }


        String[] jars = versionPath.toArray(new String[0]);
        for (String jar : jars) {
            System.out.println(jar);
        }
        List<URL> dependencyURL = new ArrayList<>();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("cdh5.cloud",
//                24646,
//                jars);

        RemoteStreamEnvironment env = new RemoteStreamEnvironment(
                "cdh5.cloud",
                24646,
                jars);
        Thread.currentThread().getContextClassLoader();
        String host = env.getHost();
        int port = env.getPort();
        System.out.println(env.getConfiguration());
        env.setParallelism(4);
        // 每 60s 做一次 checkpoint
        env.enableCheckpointing(60000);
        // checkpoint 语义设置为 EXACTLY_ONCE，这是默认语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两次 checkpoint 的间隔时间至少为 1 s，默认是 0，立即进行下一次 checkpoint
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // checkpoint 必须在 60s 内结束，否则被丢弃，默认是 10 分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只能允许有一个 checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 最多允许 checkpoint 失败 3 次
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // 当 Flink 任务取消时，保留外部保存的 checkpoint 信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
        // 允许实验性的功能：非对齐的 checkpoint，以提升性能
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        for (String jar : jars) {
            String uri = "file://" + jar;
            System.out.println(uri);
            URL url = new URL(uri);
            dependencyURL.add(url);
        }
        System.out.println("----------------------------------");
        System.out.println(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URLClassLoader newLoader = new URLClassLoader(dependencyURL.toArray(new URL[0]), classLoader);
        Thread.currentThread().setContextClassLoader(newLoader);
        String tb = "hdm821";
        String source = "create table all_type(c1 TINYINT ,c2 SMALLINT ,c3 INT ,c4 BIGINT ,c5 FLOAT ,c6 DOUBLE ,c7 BOOLEAN ,c8 DECIMAL(12,1) ,c11 STRING ,c12 TIMESTAMP(3) , PRIMARY KEY (c3) NOT ENFORCED) WITH (\n" +
                "     'connector' = 'hudi'\n" +
                "   , 'path' = 'hdfs://cdh1.cloud:8020/data/hudi//hudi/all_type'\n" +
                "   , 'write.precombine.field' = 'c12'\n" +
                "   , 'write.bucket_assign.tasks' = '4'\n" +
                "   , 'write.tasks' = '4'\n" +
                "   , 'hive_sync.enable' = 'true'\n" +
                "   , 'hive_sync.mode' = 'hms'\n" +
                "   , 'hive_sync.metastore.uris' = 'thrift://cdh1.cloud:9083'\n" +
                "   , 'hive_sync.table' = 'all_type'\n" +
                "   , 'hive_sync.db' = 'hudi'\n" +
                "   , 'hive_sync.username' = 'hive'\n" +
                "   , 'hive_sync.password' = 'q1w2e3'\n" +
                " )";
        String sink = "create table sts_hudi_hudi_all_type_topic(c1 TINYINT,c2 SMALLINT,c3 INT,c4 BIGINT,c5 FLOAT,c6 DOUBLE,c7 BOOLEAN,c8 DECIMAL(12,1),c11 STRING,c12 STRING )WITH (\n" +
                "      'connector' = 'kafka' \n" +
                "    , 'topic' = 'sts_hudi_hudi_all_type_topic' \n" +
                "    , 'scan.startup.mode' = 'earliest-offset'\n" +
                "    , 'properties.bootstrap.servers' = '10.25.10.132:9092,10.25.10.133:9092,10.25.10.131:9092'\n" +
                "    , 'properties.group.id' = 'testgroup1'\n" +
                "    , 'value.format' = 'json'\n" +
                "    , 'value.json.ignore-parse-errors' = 'true'\n" +
                "    , 'value.json.fail-on-missing-field'='false'\n" +
                "    , 'value.fields-include' = 'ALL'\n" +
                ")";
        String queryClause = "insert into all_type select c1,c2,c3,c4,c5,c6,c7,c8,c11,cast(c12 as TIMESTAMP(3) ) from sts_hudi_hudi_all_type_topic";
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        System.out.println(configuration);
        configuration.setString("pipeline.name", "hudi_test1");
        configuration.setString("akka.tcp.timeout", "1");
        configuration.setString("web.timeout", "6000");
//            configuration.setString("table.exec.async-lookup.timeout", "1");
        System.out.println(configuration);


        tableEnv.executeSql(source);
        tableEnv.executeSql(sink);
        TableResult tableResult = tableEnv.executeSql(queryClause);
        Optional<JobClient> jobClient = tableResult.getJobClient();
        JobID jobID = jobClient.get().getJobID();
        System.out.println(jobID);
        System.out.println(tableResult.getJobClient().isPresent());

    }

    public static List<String> getFileList(String path) {
        File[] filePath = FileUtil.ls(path);
        List<String> jarList = new ArrayList<>();
        for (File l : filePath) {
            if (FileUtil.isFile(l)) {
                if (StrUtil.contains(l.toString(), ".jar")) {
                    jarList.add(l.toString());
                }
            }
        }
        return jarList;
    }
}
