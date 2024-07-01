package org.example.DataStream_02;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单分流：调用 filter() 将一条 DataStream 拆分为多条。这种方式虽然简单，但同一份数据会被多次处理，效率低下。
 *
 *
 * @author Island_World
 */

public class SplitStream_simple_06_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> source = env.socketTextStream("localhost", 7777).map(Integer::valueOf);

        SingleOutputStreamOperator<Integer> s1 = source.filter(i -> i % 2 != 0);
        SingleOutputStreamOperator<Integer> s2 = source.filter(i -> i % 2 == 0);

        s1.print("奇数");
        s2.print("偶数");

        env.execute();
    }
}
