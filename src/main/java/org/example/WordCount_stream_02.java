package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流式处理
 *
 * @author Island_World
 */

public class WordCount_stream_02 {
    public static void main(String[] args) {
        // 1 创建执行环境
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 读取数据
        var textFile = env.readTextFile("input/word.txt");

        // 3 处理数据:切分、转换、分组、聚合
        var wordAndOne = textFile.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, outer) -> {
            String[] words = value.split(" ");
            for (String w : words) {
                Tuple2<String, Integer> tup = Tuple2.of(w, 1);
                outer.collect(tup);
            }
        });
        wordAndOne.keyBy()

        // 4 输出数据

        // 5 执行
    }
}
