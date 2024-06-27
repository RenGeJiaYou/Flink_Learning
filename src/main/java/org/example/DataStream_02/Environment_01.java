package org.example.DataStream_02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
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

        // 2 有时会自定义配置类，如下
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");

        // 3 设置流批不同的执行模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 4 关于 execute() 方法
        // 4-1 env.execute() 触发一个fLink job，如果一个main 方法调用了多次 env.execute()，则在第一个调用之后就会阻塞
        // 4-1 env.executeAsync() 可以异步执行，也就是说调用 n 次就会创建 n 个 job，不会阻塞
//        env.executeAsync();// 在第一个 execute() 调用之后就会阻塞

    }
}
