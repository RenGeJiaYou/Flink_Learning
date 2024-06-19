package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 单词计数
 *
 * @author Island_World
 */

public class WordCount_batch_01 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> file = env.readTextFile("input/word.txt");
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne =
                file.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                });
        // groupBy() 按照 tuple 的第[0]个元素分组，sum(1) 按照 tuple 的第[1]个元素求和
        wordAndOne.groupBy(0).sum(1).print();

    }
}
