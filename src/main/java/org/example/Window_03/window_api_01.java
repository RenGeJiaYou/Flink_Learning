package org.example.Window_03;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.Functions.WaterSensorMapFunction;
import org.pojo.WaterSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 窗口 API 介绍
 *
 * @author Island_World
 */

public class window_api_01 {
    private static final Logger log = LoggerFactory.getLogger(window_api_01.class);

    public static void main(String[] args) throws Exception {
        // 0. 数据准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(sensor -> sensor.getId());

        // todo 1. 开窗：指定窗口类型和参数
        // 1-1 未经 keyBy 的流只能通过 windowAll() 创建窗口,窗口内的所有数据进入同一个子任务，即并行度 = 1
//         sensorDS.windowAll();

        // 1-2 经过 keyBy 的流会按照 key 分为多条逻辑流，同一个 key 下的数据将发送到同一个并行子任务，被同一组窗口收集
        // 1-2-1 基于时间的窗口
//        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10))); // 滚动窗口，窗口长度 10 s
//        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))); // 滑动窗口，窗口长度 10 s，滑动步长 2 s
//        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))); // 会话窗口，超时间隔 10 s

        // 1-2-2 计数窗口
//        keyedStream.countWindow(5); // 滚动窗口，窗口长度= 5个元素
        keyedStream.countWindow(5, 2); // 滑动窗口，窗口长度= 5个元素，滑动步长= 2个元素


        keyedStream.print();
        env.execute();
    }
}
