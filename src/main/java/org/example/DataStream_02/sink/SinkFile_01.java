package org.example.DataStream_02.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;


/**
 * 使用 FlinkSink 输出到文件，将分区文件写入Flink支持的文件系统<p>
 * Flink 1.12 前（公司生产环境是 1.13.0）需要在 stream 对象上调用 addSink() 方法才能创建 sink 算子
 *
 * @author Island_World
 */

public class SinkFile_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 就用 socket 作为源算子，示例代码的 datagen 生成器不好用
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);


        // ※ 使用 FileSink 输出到文件系统
        FileSink<String> fileSink = FileSink
                // 输出行式存储的文件，指定路径(默认父目录是当前项目根目录下)、指定编码
                .<String>forRowFormat(new Path("output"), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("file_sink_pre")
                                .withPartSuffix(".log")
                                .build()
                )
                .withBucketAssigner(
                        // 每个桶实际是一个文件夹，此句控制文件夹的命名
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH 点",
                                ZoneId.systemDefault()))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 入参类型单位是毫秒
                                .withRolloverInterval(Duration.ofMinutes(5).toMillis())
                                // 入参类型单位是字节，此处是 1MB
                                .withMaxPartSize(1024 * 1024)
                                .build()
                ).build();

        source.sinkTo(fileSink);

        env.execute();
    }
}
