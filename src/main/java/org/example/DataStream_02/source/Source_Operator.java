package org.example.DataStream_02.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 01
 *
 * @author Island_World
 */

public class Source_Operator {
    public static void main(String[] args) throws Exception {
        // 0 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 从集合中读取
//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//        env.fromCollection(data).print("fromCollection");

        // 2 从文件中读取
//        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineFormat(), new Path("input/word.txt")).build();
        // noWatermarks() 常用于有界数据
//        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fromFile").print("fromFile");

        // 3 从 socket 中读取。是这种方式由于吞吐量小、稳定性较差，一般也是用于测试。
//        DataStream<String> stream = env.socketTextStream("localhost", 7777);
//        stream.print("form Socket");

        // 4 todo 从 kafka 读
        // 5 从 DataGen 读

        env.execute();

    }
}
