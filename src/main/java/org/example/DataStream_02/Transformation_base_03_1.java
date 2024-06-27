package org.example.DataStream_02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;

/**
 * 转换算子
 *
 * @author Island_World
 */

public class Transformation_base_03_1 {
    public static void main(String[] args) throws Exception {
        // 0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3));

        // 1 map
        stream.map(s -> s.id).print("map function");

        // 2 filter
        stream.filter(s -> s.vc >= 2).print("filter function");

        // 3 flatMap:如果输入数据的水位值是偶数，只打印id；如果输入数据的水位值是奇数，既打印ts又打印vc。
        stream.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if (value.vc % 2 == 0) {
                    out.collect(value.id);
                } else {
                    out.collect(value.ts.toString());
                    out.collect(value.vc.toString());
                }
            }
        }).print("flatMap function");


        env.execute();
    }
}
