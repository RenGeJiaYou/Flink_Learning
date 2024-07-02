package org.example.DataStream_02.udf;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Functions.MyFilterFunction;
import org.example.pojo.WaterSensor;

/**
 * 用户自定义函数
 *
 * @author Island_World
 */

public class base_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 3L, 3),
                new WaterSensor("sensor_3", 3L, 3));

        // 1 先分组
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(s -> s.id);

        // 2 调用自定义的 FilterFuntion 类，注意该类是有参构造
        keyedStream.filter(new MyFilterFunction("sensor_2")).print("MyFilterFunction:");


        env.execute();
    }
}
