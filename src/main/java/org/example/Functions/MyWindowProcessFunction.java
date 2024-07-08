package org.example.Functions;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;

import java.text.DateFormat;

/**
 * 全窗口函数，只在窗口最终触发时执行一次（比如只关心总数），拥有额外的上下文<br>
 * 新写法,context 提供了更多的信息，包括窗口、当前的 key 等
 *
 * @author Island_World
 */

public class MyWindowProcessFunction extends ProcessWindowFunction<WaterSensor, String, String, TimeWindow> {
    /**
     * @param key      分组的key
     * @param ctx      上下文
     * @param iterable 存的数据
     * @param out      采集器
     */
    @Override
    public void process(String key, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context ctx, Iterable<WaterSensor> iterable, Collector<String> out) throws Exception {
        long start = ctx.window().getStart();
        long end = ctx.window().getEnd();
        String s = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
        String e = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

        long count = iterable.spliterator().estimateSize();

        out.collect("key: " + key + " window: [start time:" + s + "," + "end time:" + e + ") count: " + count);
    }


}
