package org.example.DataStream_02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.pojo.WaterSensor;

/**
 * 规约算子，每个 keyBy 分组总结 「1」 条数据出来，至于这条总结数据怎么来的，可以通过 ReduceFunction 接口自定义
 *
 * @author Island_World
 */

public class Transformation_aggregation_reduce_03_4 {
    public static void main(String[] args) throws Exception {
        // 0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 3L, 3),
                new WaterSensor("sensor_3", 3L, 3));

        // 1 先分组
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(s -> s.id);

        // 2 reduce 是
        //      前后元素两两规约，v1 是先前的计算结果，v2 是醒来的那条数据；
        //      reduce()输入类型 == 输出类型；
        //      每个 key 的第一条数据来的时候，不会执行 reduce 方法,而是存起来等待第二条来时才会初次执行 reduce()
        keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(), value1.getTs(), value1.vc + value2.vc);
            }
        }).print("reduce func:");


        env.execute();
    }
}
