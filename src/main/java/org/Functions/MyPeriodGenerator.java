package org.Functions;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 周期性水位线生成器
 *
 * @author Island_World
 */

public class MyPeriodGenerator<T> implements WatermarkGenerator<T> {
    /**
     * 表示乱序程度，即能够容忍迟到的数据迟到多久
     */
    private long delayTs;

    /**
     * 从 WaterSensor 事件中提取当前为止的最大时间戳
     */
    private long maxTs;

    /**
     * 构造函数
     */
    public MyPeriodGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + delayTs + 1;
    }

    /**
     * 每条数据来，都会调用一次；用来提取最大的事件时间（如果是）
     *
     * @param event          每次到来的数据
     * @param eventTimestamp 每条数据的时间戳，本例中为 WaterSensor 的 ts 属性
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        System.out.println("calling onEvent() 获取目前为止到来事件的最大时间戳=" + maxTs);
        maxTs = Math.max(maxTs, eventTimestamp);
    }

    /**
     * 周期性调用：发射 watermark
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        System.out.println("calling onPeriodicEmit() 周期性地生成水位线=" + (maxTs - delayTs - 1));
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
    }
}
