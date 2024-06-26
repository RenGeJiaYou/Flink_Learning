package org.example.Quick_Start_01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 无界的流式计算，相较有界，仅仅在读取数据处有所不同
 *
 * @author Island_World
 */

public class WordCount_unbound_03 {
    public static void main(String[] args) throws Exception {
        // 1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 读取数据： socket ，数据来自于命令行工具 nmap
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);

        // 数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneKS = socketDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, out) -> {
            // 按空格分词
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(item -> item.f0)
                .sum(1);

        // 数据输出
        wordAndOneKS.print();

        // 执行环境
        env.execute();
    }
}
