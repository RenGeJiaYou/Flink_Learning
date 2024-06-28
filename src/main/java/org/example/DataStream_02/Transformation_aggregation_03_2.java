package org.example.DataStream_02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;

/**
 * 聚合算子，应该只用在含有有限个 key 的数据流上。
 *
 * @author Island_World
 */

public class Transformation_aggregation_03_2 {
    public static void main(String[] args) throws Exception {
        // 0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 3L, 3),
                new WaterSensor("sensor_3", 3L, 3));

        // 1 keyBy 是聚合前必须要做的操作，哈希指定的key进行分组
        // keyBy() 将 DataStream 转换为 KeyedStream，只有 KeyedStream 才能调用 reduce() 和 sum() 等聚合算子
        stream.keyBy(s->s.id)
                .max("vc")
                .print("keyBy function");



        env.execute();
    }
}
