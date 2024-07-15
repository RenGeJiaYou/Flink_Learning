package org.example.ProcessFunction_05;

import org.Functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.pojo.WaterSensor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 在 ProcessAllWindowFunction 中，
 * 1. 强行将所有数据放在一个分区上进行了开窗操作。这相当于将并行度强行设置为 1，这种操作需要极力避免
 * 2. 在全窗口函数中定义了一个 hashmap，遍历所有数据才能更新 hashmap，最后才算 可能会比较慢
 * 针对这两点，可以分别从两个方面进行优化：
 * 1. 使用 KeyedProcessFunction，将数据按照 vc 分区，避免将所有数据放在一个分区上
 * 2. 使用 增量聚合函数，不要等到最后才算
 *
 * @author Island_World
 */

public class Keyed_Process_Function_TopN_02_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 容忍 3 秒的乱序数据
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 将 s 单位转换为 ms 单位
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
                );

        /*
          思路二：
          1. 使用 KeyedProcessFunction 按照 vc 分组、开窗、聚合（增量计算+全量打标签），输出的数据结构为 Tuple3<vc , 出现次数（count） , 窗口结束时间>

          */
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggStream = stream.keyBy(WaterSensor::getVc)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new VcCountAgg(),
                        new WindowResult()
                );

        /*
        2. 按照窗口标签（窗口结束时间）keyby，保证同一个窗口时间范围的结果，到一起去。排序、取TopN
           =》 按照 windowEnd做 keyby
           =》 使用 process， 来一条调用一次，需要先存，分开存，用 HashMap,key=windowEnd,value=List
           =》 使用定时器，对存起来的结果 进行排序、取前N个
           */
        aggStream
                .keyBy(r -> r.f2)
                .process(new TopN(2))
                .print();
        env.execute();
    }

    /**
     * 按照 vc 进行聚合，一个窗口内有 n 个元素，将执行 n-1 次 add()
     * */
    private static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return 0;
        }
    }

    /**
     * 泛型如下：
     * Integer：输入类型 = 增量函数的输出  count值<br>
     * Tuple3：输出类型 = Tuple3(vc，count，windowEnd) ,带上 窗口结束时间 的标签 <br>
     * Integer：key类型 ， vc <br>
     * TimeWindow：窗口类型 <br>
     */
    private static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {
        @Override
        public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context ctx, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            Integer count = elements.iterator().next();
            long windowEnd = ctx.window().getEnd();
            out.collect(Tuple3.of(key, count, windowEnd));
        }
    }

    /**
     * Long: 定时器的时间戳
     * Tuple3<Integer, Integer, Long>: 窗口内的数据: vc, count, windowEnd
     * String: 输出结果
     */
    private static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        public TopN(int threshold) {
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }
    }
}