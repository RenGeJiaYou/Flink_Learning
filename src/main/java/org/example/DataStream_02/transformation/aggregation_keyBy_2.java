package org.example.DataStream_02.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.pojo.WaterSensor;

/**
 * 聚合算子，应该只用在含有有限个 key 的数据流上。
 *
 * @author Island_World
 */

public class aggregation_keyBy_2 {
    public static void main(String[] args) throws Exception {
        // 0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 3L, 3),
                new WaterSensor("sensor_3", 3L, 3));

        // 1 keyBy 是聚合前必须要做的操作，哈希指定的key进行分组
        // keyBy() 将 DataStream 转换为 KeyedStream，只有 KeyedStream 才能调用 reduce() 和 sum() 等聚合算子
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(s -> s.id);

        keyedStream.sum("vc").print();


        env.execute();
    }
}
/* 输出结果分析：
    * WaterSensor(id=sensor_1, ts=1, vc=1)
    * WaterSensor(id=sensor_2, ts=2, vc=2)
    * WaterSensor(id=sensor_2, ts=2, vc=5) ✳ 重点是这一条，sum()累加原有的 vc=2 和 现有的 vc=3.
    * WaterSensor(id=sensor_3, ts=3, vc=3)
    *
    * 1. Flink 流式处理是一条计算完计算下一条的
    * 2. FLink 对每一个单独的 key 都会记录一个状态
    * */