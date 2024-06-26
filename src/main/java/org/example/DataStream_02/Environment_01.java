package org.example.DataStream_02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 01
 *
 * @author Island_World
 */

public class Environment_01 {
    public static void main(String[] args) {
        // 1 创建环境
        // getExecutionEnvironment() 在底层调用时传入了一个默认的配置信息
        // 开发中直接用就行
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //
    }
}
