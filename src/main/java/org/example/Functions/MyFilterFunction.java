package org.example.Functions;

import lombok.AllArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.example.pojo.WaterSensor;

/**
 * 根据传参过滤出指定记录。
 * 具体做法是在自定义类中创建对应成员变量及构造函数
 *
 * @author Island_World
 */
@AllArgsConstructor
public class MyFilterFunction implements FilterFunction<WaterSensor> {

    public String id;

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }
}
