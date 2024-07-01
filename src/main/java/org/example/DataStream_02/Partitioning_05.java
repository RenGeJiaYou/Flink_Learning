package org.example.DataStream_02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区：流式的一大堆数据如何分给某个算子的 n 个并行子任务？
 *
 * @author Island_World
 */

public class Partitioning_05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度=2 即存在 2 个并行子任务
        env.setParallelism(3);

        // 在终端执行 ncat -lk 7777,并依次输入 1~10
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        // 随机分区
//        source.shuffle().print();
        // 轮询分区
//        source.rebalance().print();
        // 缩放轮询:按照 n 个源也将下游算子的所有子任务分为 n 个组，每一个源在该组内轮询。
        source.rescale().print();  

        /**
         * 假设有两个数据源 source[0],输入数据1~8;source[1],输入数据11~18
         * t0~t3 四个下游算子，
         * rebalance():
         *  t0: 1↓   ↗5↓ | 11↓   ↗15↓
         *  t1: 2↓  ↗ 6↓ | 12↓  ↗ 16↓
         *  t2: 3↓ ↗  7↓ | 13↓ ↗  17↓
         *  t3: 4↓↗   8↓ | 14↓↗   18↓
         *  ===============
         *  rescale():
         *  t0: 1↓ ↗3↓  5↓  7
         *  t1: 2↓↗ 4↓↗ 6↓↗ 8
         *  ——————————————————
         *  t3: 11 13 15 17
         *  t4: 12 14 16 18
         * */
        source.rescale().print();


        env.execute();
    }
}
/*
 * 2分区，经过 shuffle()，完全随机地将数据分配给子任务
*   1> 1
    2> 2
    2> 3
    1> 4
    2> 5
    2> 6
    2> 7
    2> 8
    1> 9
    1> 10
    2> 11
    2> 12
    2> 13

    2分区，经过 rebalance()，轮询地将数据分配给子任务
    1> 1
    2> 2
    1> 3
    2> 4
    1> 5
    2> 6
* */
