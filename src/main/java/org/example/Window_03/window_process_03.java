package org.example.Window_03;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.Functions.MyWindowProcessFunction;
import org.Functions.WaterSensorMapFunction;
import org.pojo.WaterSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 窗口 API 介绍
 *
 * @author Island_World
 */

public class window_process_03 {
    private static final Logger log = LoggerFactory.getLogger(window_process_03.class);

    public static void main(String[] args) throws Exception {
        // 0. 数据准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(sensor -> sensor.getId());


        // todo 窗口函数：aggregate
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        windowedStream
//                .apply(new MyWindowApplyFunction()) // 老写法
                .process(new MyWindowProcessFunction())
                .print();

        env.execute();
    }
}
/**
 * 不同速率输入 s1,1,1
 * <p>
 * 输出：
 * key: s1 window: [start time:2024-07-08 20:14:40,end time:2024-07-08 20:14:50) count: 2
 * key: s1 window: [start time:2024-07-08 20:14:50,end time:2024-07-08 20:15:00) count: 6
 * key: s1 window: [start time:2024-07-08 20:15:00,end time:2024-07-08 20:15:10) count: 4
 * key: s1 window: [start time:2024-07-08 20:15:10,end time:2024-07-08 20:15:20) count: 2
 */