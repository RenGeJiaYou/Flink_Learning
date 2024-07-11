package org.Functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.pojo.WaterSensor;

/**
 * @author Island_World
 */

/**
 * 将每一行文本数据按","分割，封装成 WaterSensor 类型
 * */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }
}
