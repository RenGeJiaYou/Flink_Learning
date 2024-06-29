package org.example.DataStream_02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.pojo.WaterSensor;

/**
 * 聚合算子，应该只用在含有有限个 key 的数据流上。
 *
 * @author Island_World
 */

public class Transformation_aggregation_minBy_03_3 {
    public static void main(String[] args) throws Exception {
        // 0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 3L, 3),
                new WaterSensor("sensor_3", 3L, 3));


        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(s -> s.id);

        keyedStream.maxBy("vc").print("maxBy func:");


        env.execute();
    }
}
/* 输出结果分析：
    max：
        max func:> WaterSensor(id=sensor_1, ts=1, vc=1)
        max func:> WaterSensor(id=sensor_2, ts=2, vc=2)
        max func:> WaterSensor(id=sensor_2, ts=2, vc=3)
        max func:> WaterSensor(id=sensor_3, ts=3, vc=3)

    maxBy:
        maxBy func:> WaterSensor(id=sensor_1, ts=1, vc=1)
        maxBy func:> WaterSensor(id=sensor_2, ts=2, vc=2)
        maxBy func:> WaterSensor(id=sensor_2, ts=3, vc=3)
        maxBy func:> WaterSensor(id=sensor_3, ts=3, vc=3)

    1.min()/max()/minBy()/maxBy() 涉及范围是同一个 key 下的所有记录
    2.min("fieldA") / minBy("fieldA") 的区别就在于要不要取非比较字段 ("fieldB")、("fieldC")... 的值：
        1) min:  当有新的 fieldA 最小值出现时，只取该字段值。输出记录的 fieldB、fieldC... 还是旧的最小值
        2) minBy:当有新的 fieldA 最小值出现时，把这含有 fieldA 最小值的整条记录都输出
    * */