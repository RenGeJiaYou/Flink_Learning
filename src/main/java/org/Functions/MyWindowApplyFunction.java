package org.Functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.pojo.WaterSensor;

/**
 * 全窗口函数，只在窗口最终触发时执行一次（比如只关心总数），拥有额外的上下文<br>
 * 老写法
 *
 * @author Island_World
 */

public class MyWindowApplyFunction implements WindowFunction<WaterSensor, String, String, TimeWindow> {
    /**
     * @param key        分组的key
     * @param timeWindow 窗口对象
     * @param iterable   存的数据
     * @param out        采集器
     */
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<WaterSensor> iterable, Collector<String> out) throws Exception {
        System.out.println("MyWindowProcessFunction called");
        int count = 0;
        for (WaterSensor waterSensor : iterable) {
            count++;
        }
        out.collect("key: " + key + " count: " + count);
    }
}
