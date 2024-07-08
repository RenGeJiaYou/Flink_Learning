package org.example.Window_03;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example.Functions.WaterSensorMapFunction;
import org.example.pojo.WaterSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 窗口 API 介绍
 *
 * @author Island_World
 */

public class window_incremental_aggregation_02_1 {
    private static final Logger log = LoggerFactory.getLogger(window_incremental_aggregation_02_1.class);

    public static void main(String[] args) throws Exception {
        // 0. 数据准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(sensor -> sensor.getId());


        // todo 指定窗口函数：要如何统计窗口中的一堆数据
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 2-1 增量聚合：窗口中每来一条数据，就进行一次计算，窗口触发时输出计算结果
        SingleOutputStreamOperator<WaterSensor> reduced = windowedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("calling reduce()" + value1.getVc() + " " + value2.getVc());
                return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
            }
        });

        reduced.print();
        env.execute();
    }
}
/**
 * 在十余秒内输入：
 * a,1,1 ┓
 * a,1,2 ┃> 第一个 10s
 * a,1,3 ┛
 * a,1,4 ┓
 * a,1,5 ┃> 第二个 10s
 * a,1,6 ┛
 * ===============================================
 * 输出结果：
 * calling reduce()1 2              |
 * calling reduce()3 3              |
 * WaterSensor(id=a, ts=1, vc=6)    |=> 10s窗口触发一次 执行 print()，当前窗口内数据累加
 * calling reduce()4 5              |
 * calling reduce()9 6              |
 * WaterSensor(id=a, ts=1, vc=15)   |=> 10s窗口触发一次 执行 print()，当前窗口内数据累加
 */