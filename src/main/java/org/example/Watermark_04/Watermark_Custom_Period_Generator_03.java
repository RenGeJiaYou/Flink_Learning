package org.example.Watermark_04;

import org.Functions.MyPeriodGenerator;
import org.Functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.pojo.WaterSensor;

import java.time.Duration;

/**
 * 周期性生成器一般是通过 onEvent() 观察判断输入的事件，而在 onPeriodicEmit() 里发出水位线。
 *
 * @author Island_World
 */

public class Watermark_Custom_Period_Generator_03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线生成的周期为 200 ms
        env.getConfig().setAutoWatermarkInterval(2000);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        // 1 定义 Watermark 策略, 乱序流的内置水位线调用 forBoundedOutOfOrderness()
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1-1 指定 Watermark 自定义生成器
                .<WaterSensor>forGenerator((WatermarkGeneratorSupplier<WaterSensor>) ctx -> new MyPeriodGenerator<>(3000L))
                // 1-2 指定时间戳分配器，从数据中提取
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (ws, timestamp) -> {
                    // 返回的时间戳，单位 ms
                    System.out.println("乱序流水位线数据=" + ws + ",recordTs=" + timestamp);
                    return ws.getTs() * 1000L;
                });

        // 2 指定 Watermark 策略
        SingleOutputStreamOperator<WaterSensor> streamWithWatermark = stream.assignTimestampsAndWatermarks(watermarkStrategy);

        // 3 对内置有序水位线的流 使用事件时间语义的窗口
        streamWithWatermark.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context ctx, Iterable<WaterSensor> items, Collector<String> out) throws Exception {
                        long start = ctx.window().getStart();
                        long end = ctx.window().getEnd();
                        String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

                        // 实际中的流常常是无限的，真的遍历一次成本太高。所以我们可以通过 estimateSize() 方法来估算（而不必真的遍历）元素的数量
                        items.spliterator().estimateSize();

                        out.collect("key: " + key + " window: [" + startTime + ", " + endTime + ") count: " + items.spliterator().estimateSize());
                    }
                })
                .print();

        env.execute();
    }
}
