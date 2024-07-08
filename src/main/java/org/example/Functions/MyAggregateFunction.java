package org.example.Functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.pojo.WaterSensor;

/**
 * 窗口函数：增量聚合 aggregate 的自定义实现<p>
 * 功能是将 WaterSensor 中的 vc 字段累加,并返回累加结果的字符串
 *
 * @author Island_World
 */


public class MyAggregateFunction implements AggregateFunction<WaterSensor, Integer, String> {
    @Override
    public Integer createAccumulator() {
        System.out.println("创建createAccumulator()");
        return 0;
    }

    /**
     * @param waterSensor 当前来的一条数据
     * @param acc         之前已经累加的数据
     */
    @Override
    public Integer add(WaterSensor waterSensor, Integer acc) {
        System.out.println("调用add(),且当前累加值为:"+acc+", 当前来的值为"+waterSensor.getVc());

        return acc + waterSensor.getVc();
    }

    /**
     * @param acc 窗口触发时，所有数据累加的结果
     * @return 返回窗口计算最终结果
     */
    @Override
    public String getResult(Integer acc) {
        System.out.println("调用getResult()");
        return acc.toString();
    }

    // 只有会话窗口才会调用
    @Override
    public Integer merge(Integer integer, Integer acc1) {
        System.out.println("调用merge()");
        return 0;
    }
}
