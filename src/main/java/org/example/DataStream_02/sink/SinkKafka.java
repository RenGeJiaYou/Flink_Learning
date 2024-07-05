package org.example.DataStream_02.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.time.ZoneId;


/**
 * 使用 FlinkSink 输出到文件，将分区文件写入Flink支持的文件系统<p>
 * Flink 1.12 前（公司生产环境是 1.13.0）需要在 stream 对象上调用 addSink() 方法才能创建 sink 算子
 *
 * @author Island_World
 */

public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 如果是精准一次，必须开启checkpoint（后续章节介绍）
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        // 就用 socket 作为源算子，示例代码的 datagen 生成器不好用
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        // ※ 输出到 kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
                .setBootstrapServers("localhost:9092")
                // 指定序列化器：指定Topic名称、具体的序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 写到 kafka 的一致性级别： 精准一次、至少一次
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("kafka_transaction-")
                .build();


        source.sinkTo(kafkaSink);

        env.execute();
    }
}
