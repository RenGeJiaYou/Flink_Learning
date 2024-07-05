package org.example.DataStream_02.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.Functions.WaterSensorMapFunction;
import org.example.pojo.WaterSensor;

import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * 该场景最常用
 * 使用 FlinkSink 输出到文件，将分区文件写入Flink支持的文件系统<p>
 * Flink 1.12 前（公司生产环境是 1.13.0）需要在 stream 对象上调用 addSink() 方法才能创建 sink 算子
 *
 * @author Island_World
 */

public class SinkMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> source = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());


        // 选择 MySQL 作为 sink
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into ws values(?,?,?)",
                (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(100) // 批次的大小：条数
                        .withBatchIntervalMs(3000) // 批次的时间
                        .build(),
                // JdbcConnectionOptions 的内部类 JdbcConnectionOptionsBuilder 需要 new 一下
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.1.108:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("hbjy6_java")
                        .withPassword("zTHBdczCCbTx")
                        .build()
        );
        source.addSink(jdbcSink);

        env.execute();
    }
}
