package org.example.ProcessFunction_05;

import org.Functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.pojo.WaterSensor;

import java.time.Duration;

/**
 * KeyedProcessFunction 的定时器使用
 *
 * @author Island_World
 */

public class Keyed_Process_Function_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
                );

        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(WaterSensor::getId);

        // todo 开始 Process:keyed
        keyedStream.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    /**
                     * 来一条数据调用一次
                     * @param ws 来的每一个 WaterSensor 实例
                     * @param ctx 当前 keyed 流的上下文
                     * @param out 采集器
                     * */
                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前数据的 key
                        String currentKey = ctx.getCurrentKey();
                        // 获取当前数据的时间戳（事件时间）
                        Long currentTimeStamp = ctx.timestamp();

                        // todo 1. 通过 TimeService 定时器注册(事件时间)
                        ctx.timerService().registerEventTimeTimer(5000L);
                        System.out.println("当前的 key 是" + currentKey + "当前时间戳(事件时间)是" + currentTimeStamp + ", 注册了一个 5 s 的定时器");


//                        ctx.timerService().registerProcessingTimeTimer(1000); // 有对应的处理时间注册

                        // 通过 TImeService 获取当前处理时间/水位线
//                        ctx.timerService().currentProcessingTime();
//                        ctx.timerService().currentWatermark();

                        // 通过 TImeService 删除 timer
//                        ctx.timerService().deleteEventTimeTimer();
//                        ctx.timerService().deleteProcessingTimeTimer();
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        System.out.println("当前 timer 所属的 key 是"+ctx.getCurrentKey()+";当前时间是" + ctx.timestamp() + ", 定时器触发");
                    }
                }
        ).print();
        env.execute();
    }
}
/**
 * 输入用例一：单一的 key
 *
 * registerEventTimeTimer() 在 processElement() 内是单例的，多次调用 processElement() 只是在更新同一个 TimeService 实例，而不会多次创建
 * 输入     | 输出                                                       | 描述
 * s1,1,1  | 当前的 key 是s1当前时间戳(事件时间)是1000, 注册了一个 5 s 的定时器 | 定时器设在 1 + 5 = 6秒，如果接下来接收一个时间戳 >6 的数据，将触发 omTimer() 内定时逻辑
 * s1,2,1  | 当前的 key 是s1当前时间戳(事件时间)是2000, 注册了一个 5 s 的定时器 | 定时器设在 2 + 5 = 7秒，如果接下来接收一个时间戳 >7 的数据，将触发 omTimer() 内定时逻辑
 * s1,3,1  | 当前的 key 是s1当前时间戳(事件时间)是3000, 注册了一个 5 s 的定时器 | 定时器设在 3 + 5 = 8秒，如果接下来接收一个时间戳 >8 的数据，将触发 omTimer() 内定时逻辑
 * s1,9,1  | 当前的 key 是s1当前时间戳(事件时间)是9000, 注册了一个 5 s 的定时器 | 该数据的时间戳 9 > 8，触发 onTimer()
 *
 * 输入用例二：流内数据有多个不同的 key
 * | `s1,1,1` | `当前的 key 是s1当前时间戳(事件时间)是1000, 注册了一个 5 s 的定时器`                                                                                                                                  |
 * | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
 * | `s1,2,2` | `当前的 key 是s1当前时间戳(事件时间)是2000, 注册了一个 5 s 的定时器`                                                                                                                                  |
 * | `s1,3,3` | `当前的 key 是s1当前时间戳(事件时间)是3000, 注册了一个 5 s 的定时器`                                                                                                                                  |
 * | `s2,4,4` | `当前的 key 是s2当前时间戳(事件时间)是4000, 注册了一个 5 s 的定时器`                                                                                                                                  |
 * | `s2,5,5` | `当前的 key 是s2当前时间戳(事件时间)是5000, 注册了一个 5 s 的定时器`                                                                                                                                  |
 * | `s3,9,9` | `当前的 key 是s3当前时间戳(事件时间)是9000, 注册了一个 5 s 的定时器`<br>`当前 timer 所属的 key 是s1;当前时间是5000, 定时器触发`<br>`当前 timer 所属的 key 是s3;当前时间是5000, 定时器触发`<br>`当前 timer 所属的 key 是s2;当前时间是5000, 定时器触发` |
 * */