package org.example.quick_start_01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流式处理
 *
 * @author Island_World
 */

public class WordCount_stream_02 {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String inputPath = "input/word.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        // 按照第一个位置的 word 分组，item 是 Tuple2 的一个实例，f0 是第一个位置的数据，f1 是第二个位置的数据
        // 按照第二个位置上的数据求和
        DataStream<Tuple2<String, Integer>> dataStream = inputDataStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, out) -> {
                    // 按空格分词
                    String[] words = s.split(" ");
                    // 遍历所有word，包成二元组输出
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(item -> item.f0)
                .sum(1);
        dataStream.print();

        //执行任务
        env.execute();

        /*
         * 流处理会将所有中间过程都会被输出，前面的序号就是并行执行任务的线程编号。输出为：
         * 3> (hello,1)
         * 7> (flink,1)
         * 5> (world,1)
         * 3> (hello,2)
         * 1> (kafka,1)
         * 8> (hadoop,1)
         * 3> (hello,3)
         * 1> (spark,1)
         * */
    }
}


