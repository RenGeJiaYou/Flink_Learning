package org.example.Window_03;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example.Functions.MyAggregateFunction;
import org.example.Functions.WaterSensorMapFunction;
import org.example.pojo.WaterSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 窗口 API 介绍
 *
 * @author Island_World
 */

public class window_api_aggregation_01_2 {
    private static final Logger log = LoggerFactory.getLogger(window_api_aggregation_01_2.class);

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
                .aggregate(new MyAggregateFunction())
                .print();

        env.execute();
    }
}
/**
 * 输入数据：| 输出结果：
 * s1,1,1  | 创建createAccumulator()、调用add(),且当前累加值为:0, 当前来的值为1
 * s1,1,2  | 调用add(),且当前累加值为:1, 当前来的值为2
 * s1,1,3  | 调用add(),且当前累加值为:3, 当前来的值为3
 * s1,1,4  | 调用add(),且当前累加值为:6, 当前来的值为4、【窗口期 10s 到期】、调用getResult()、10
 * s1,1,5  | 创建createAccumulator()、调用add(),且当前累加值为:0, 当前来的值为5
 * s1,1,6  | 调用add(),且当前累加值为:5, 当前来的值为6
 * s1,1,7  | 调用add(),且当前累加值为:11, 当前来的值为7
 * s1,1,8  | 调用add(),且当前累加值为:18, 当前来的值为8、【窗口期 10s 到期】、调用getResult()、26
 * s1,1,9  | 创建 createAccumulator()、调用add(),且当前累加值为:0, 当前来的值为9
 * */