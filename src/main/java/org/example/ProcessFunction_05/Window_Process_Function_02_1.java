package org.example.ProcessFunction_05;

import javafx.scene.input.DataFormat;
import org.Functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.pojo.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * ProcessWindowFunction 也可以利用 context 获取一些上下文
 * 但它和 KeyedProcessFunction 的区别是：context 没有定时器相关函数
 * <p>
 * 本类以一个 Top N 案例演示 ProcessWindowFunction 的使用：
 * 统计最近 10 秒钟内出现次数最多的两个水位，并且每5秒钟更新一次。
 * 我们知道，这可以用一个滑动窗口来实现。
 *
 * @author Island_World
 */

public class Window_Process_Function_02_1 {
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

        /**
         * 思路一：不区分不同水位，而是将所有访问数据都收集起来，统一进行统计计算。
         * 每个窗口触发时，使用 hashmap 搜集数据，key=vc，value=count 值
         * */
        stream.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyTopNPAWF())
                .print();

        env.execute();
    }

    public static class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context ctx, Iterable<WaterSensor> iterable, Collector<String> out) throws Exception {
            // 定义一个hashmap用来存，key=vc，value=count值
            HashMap<Integer, Integer> map = new HashMap<>();
            // 1.遍历数据, 统计 各个vc出现的次数
            for (WaterSensor i : iterable) {
                Integer vc = i.getVc();
                if (map.containsKey(vc)) {
                    map.put(vc, map.get(vc) + 1);
                } else {
                    map.put(vc, 1);
                }
            }

            // 2. 数据转移到 List 中，对 count 排序
            List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
            for (Integer vc : map.keySet()) {
                datas.add(Tuple2.of(vc, map.get(vc)));
            }
            // 降序
            datas.sort((o1, o2) -> o2.f1 - o1.f1);

            // 3. 输出  count 最大的2个 vc
            StringBuilder outStr = new StringBuilder();

            outStr.append("================================\n");
            for (int i = 0; i < Math.min(2, datas.size()); i++) {
                outStr.append("Top" + (i + 1) + "\n");
                outStr.append("vc: " + datas.get(i).f0 + "\n");
                outStr.append("count: " + datas.get(i).f1 + "\n");
                outStr.append("窗口结束时间" + DateFormatUtils.format(ctx.window().getEnd(), "yyyy 年 MM 月 dd 日 HH:mm:ss.SSS") + "\n");
                outStr.append("================================\n");
            }
            out.collect(outStr.toString());
        }
    }
}
/**
 * 总结：
 * 1. 代码中设置为事件时间窗口，意味着水位线上涨取决于新到数据的时间戳，而不是物理时间。
 * 2. 容忍 3 秒的乱序数据：如果最新的事件时间戳是 T，那么水位线将是 T - 3 秒。只有当水位线超过窗口的结束时间时，窗口才会被触发。
 * 3. 设置了滑动窗口（size=10,slide=5），第一个窗口是 -5~5 秒（！），第二个窗口是 0-10 秒，以此类推。
 *
 * 输入    | 输出
 * s1,1,1 | 无输出，第一条数据来的时候，水位线初始值为 Long.MIN_VALUE，不会触发窗口
 * s1,1,2 | 无输出，第二条数据来的时候，水位线为 1-3 = -2 秒，不会触发窗口
 * s1,1,2
 * s1,1,3
 * s1,1,3
 * s1,1,3
 * s1,1,3
 * s1,1,3
 * s1,1,4
 * s1,1,4
 * s1,1,5
 * s1,1,6
 * s1,1,7
 * s1,1,8
 * s1,13,8 | 水位线提升到 13 - 3 = 10 秒，触发第 1（第5秒结束）、2（第10秒结束）个窗口，输出结果
 * s1,18,8 | 水位线提升到 18 - 3 = 15 秒，触发第 3 个窗口，输出结果
 * s1,23,8 | 水位线提升到 23 - 3 = 20 秒，触发第 4 个窗口，输出结果
 * s1,33,42| 水位线提升到 33 - 3 = 30 秒，触发第 5、6 个窗口，输出结果
 * s1,43,42| 水位线提升到 43 - 3 = 40 秒，触发第 7、8 个窗口，输出结果
 * */

