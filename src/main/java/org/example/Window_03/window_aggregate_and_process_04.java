package org.example.Window_03;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.Functions.MyAggregateFunction;
import org.example.Functions.MyWindowProcessFunction;
import org.example.Functions.WaterSensorMapFunction;
import org.example.pojo.WaterSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 增量聚合 Aggregate + 全窗口 process <br>
 * 1、增量聚合函数处理数据： 来一条计算一条<br>
 * 2、窗口触发时， 增量聚合的结果（只有一条） 传递给 全窗口函数<br>
 * 3、经过全窗口函数的处理包装后，输出<br>
 * <p>
 * 结合两者的优点：<br>
 * 1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少<br>
 * 2、全窗口函数： 可以通过 上下文 实现灵活的功能<br>
 *
 * @author Island_World
 */

public class window_aggregate_and_process_04 {
    private static final Logger log = LoggerFactory.getLogger(window_aggregate_and_process_04.class);

    public static void main(String[] args) throws Exception {
        // 0. 数据准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(sensor -> sensor.getId());


        // 1. 开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 2. 增量聚合 + 全窗口函数,写在 aggregate() 中
        // 注意 MyAggregateFunction 的 OUT 类型要和 MyWindowProcessFunction 的输入类型一致
        windowedStream
                .aggregate(new MyAggregateFunction(), new MyProcess())
                .print();

        env.execute();
    }

    /**
     * 第一个泛型是输入类型 String,等同于 AggregateFunction 的输出类型
     * */
    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<String, String, String, TimeWindow>.Context ctx, Iterable<String> elements, Collector<String> out) throws Exception {
            long startTs = ctx.window().getStart();
            long endTs = ctx.window().getEnd();
            String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = elements.spliterator().estimateSize();

            out.collect("key: " + key + " window: [start time:" + windowStart + ",end time:" + windowEnd + ") count: " + count);
        }
    }
}
/** 输入：不同快慢地输入 “s1,1,1”
 * 特别要关心的是，为什么 MyProcess 的输出 count 都是 1
 * 因为 AggregateFunction 传给 ProcessWindowFunction 的是 1 个聚合结果，而不是一堆数据
 * ===========================================================================
 * 输出结果：
 * 创建createAccumulator()
 * 调用add(),且当前累加值为:0, 当前来的值为1
 * 调用add(),且当前累加值为:1, 当前来的值为1
 * 调用add(),且当前累加值为:2, 当前来的值为1
 * 调用getResult()
 * key: s1 window: [start time:2024-07-08 20:39:10.000,end time:2024-07-08 20:39:20.000) count: 1
 * 创建createAccumulator()
 * 调用add(),且当前累加值为:0, 当前来的值为1
 * 调用add(),且当前累加值为:1, 当前来的值为1
 * 调用add(),且当前累加值为:2, 当前来的值为1
 * 调用add(),且当前累加值为:3, 当前来的值为1
 * 调用add(),且当前累加值为:4, 当前来的值为1
 * 调用add(),且当前累加值为:5, 当前来的值为1
 * 调用add(),且当前累加值为:6, 当前来的值为1
 * 调用add(),且当前累加值为:7, 当前来的值为1
 * 调用add(),且当前累加值为:8, 当前来的值为1
 * 调用add(),且当前累加值为:9, 当前来的值为1
 * 调用add(),且当前累加值为:10, 当前来的值为1
 * 调用add(),且当前累加值为:11, 当前来的值为1
 * 调用add(),且当前累加值为:12, 当前来的值为1
 * 调用add(),且当前累加值为:13, 当前来的值为1
 * 调用getResult()
 * key: s1 window: [start time:2024-07-08 20:39:20.000,end time:2024-07-08 20:39:30.000) count: 1
 * */
