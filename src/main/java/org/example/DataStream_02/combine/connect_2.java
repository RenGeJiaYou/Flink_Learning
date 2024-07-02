package org.example.DataStream_02.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 基本合流操作<p>
 * 连接（Connect）: 将两条 *数据类型不同* 的 DataStream 合并为「形式上的」一条 DataStream，实质还是独立的两条流<p>
 *
 * 注意
 * 1. 一次只能连接 2 条流
 * 2. 连接的两条流可以是不同的数据类型
 * 3. 连接后可以调用 map、flatmap、process来处理，但是各处理各的（实现 Co- 接口）
 *
 * @author Island_World
 */

public class connect_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.fromElements(1, 3, 5, 7, 9);
        DataStreamSource<String> s2 = env.fromElements("2", "4", "6", "8", "10");

        ConnectedStreams<Integer, String> s = s1.connect(s2);
        s.map(new CoMapFunction<Integer, String, String>() {
            // 两条流的处理逻辑
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流:" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字母流:" + value;
            }
        }).print();

        env.execute();
    }
}
/**
 * 输出结果：
 来源于字母流:2
 来源于数字流:1
 来源于字母流:4
 来源于数字流:3
 来源于字母流:6
 来源于数字流:5
 来源于字母流:8
 来源于数字流:7
 来源于字母流:10
 来源于数字流:9
 */