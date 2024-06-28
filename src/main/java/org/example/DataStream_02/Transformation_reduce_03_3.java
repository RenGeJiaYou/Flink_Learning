package org.example.DataStream_02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Functions.WaterSensorMapFunction;
import org.example.pojo.WaterSensor;

/**
 * 聚合算子，应该只用在含有有限个 key 的数据流上。
 *
 * @author Island_World
 */

public class Transformation_reduce_03_3 {
    public static void main(String[] args) throws Exception {
        // 0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .keyBy(s -> s.id)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor w1, WaterSensor w2) throws Exception {
                        System.out.println("reduce function");
                        int maxVc = Math.max(w1.getVc(), w2.getVc());

                        // 手写 max() 函数
//                                w1.setVc(maxVc);
                        // 手写 maxBy() 函数
                        if (w1.getVc() == maxVc) {
                            return w1;
                        } else {
                            return w2;
                        }
                    }
                })
                .print();
        env.execute();
    }


}
