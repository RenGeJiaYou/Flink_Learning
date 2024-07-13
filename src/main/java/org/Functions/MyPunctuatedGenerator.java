package org.Functions;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 断点式水位线生成器
 * 普通的周期性水位线，每隔固定时间更新一次水位线
 * 断点式水位线:每次有事件来都更新一次水位线，因为 onEvent() 也有 output 参数，该逻辑直接写在 onEvent() 函数
 *
 * @author Island_World
 */

public class MyPunctuatedGenerator<T> implements WatermarkGenerator<T> {
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
    public MyPunctuatedGenerator(long delayTs) {
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
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
    }

    /**
     * 周期性调用：不再需要
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}
