package org.example.DataStream_02.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基本合流操作<p>
 * 联合（union）: 将两条或多条 *数据类型相同* 的 DataStream 合并为一条 DataStream
 * 合并结果无序
 *
 * @author Island_World
 */

public class union_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.fromElements(1,3,5,7,9);
        DataStreamSource<Integer> s2 = env.fromElements(2,4,6,8,10);

        DataStream<Integer> s = s1.union(s2);
        s.print();

        env.execute();
    }
}
/**
 * 输出结果：
 * 2> 1
 * 3> 3
 * 3> 4
 * 5> 7
 * 4> 6
 * 4> 5
 * 2> 2
 * 6> 10
 * 6> 9
 * 5> 8
 * */