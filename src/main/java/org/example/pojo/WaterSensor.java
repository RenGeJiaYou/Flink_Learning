package org.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 水位传感器类
 *
 * @author Island_World
 */
@Data
@AllArgsConstructor
public class WaterSensor {
    /**
     * 水位传感器类型
     * */
    public String id;

    /**
     * 传感器记录时间戳
     * */
    public Long ts;

    /**
     * 传感器水位值
     * */
    public Integer vc;
}
