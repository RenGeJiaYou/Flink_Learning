package org.example.DataStream_02.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * keyBy 合流操作<p>
 * 连接两条流，输出能根据id匹配上的数据（类似inner join效果）<p>
 * <p>
 * 第一步：用 source1.connect(source2) 连接两条流
 * 第二步：用 connect.keyBy(s1 -> s1.f0, s2 -> s2.f0) 根据关联条件进行 keyBy
 * 第三步※：用 keyByCoStream.process() 实现互相匹配的效果
 * - 如果 A B 两条流中有 id 一致的两条数据，就输出这两条数据,但是谁的数据先来是不一定的
 * - 为此需要为两个流各设立一条缓存，用来存储各自的数据，A 流来一条数据时，就将其存入 A 缓存中，然后查看 B 缓存中是否有匹配的数据就一起输出；反之亦然。
 * <p>
 * todo 这一部分不太懂
 *
 * @author Island_World
 */

public class connect_keyBy_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "甲一", 1),
                Tuple3.of(1, "甲二", 2),
                Tuple3.of(2, "乙", 1),
                Tuple3.of(3, "丙", 1)
        );

        ConnectedStreams<
                Tuple2<Integer, String>,
                Tuple3<Integer, String, Integer>
                > connect = source1.connect(source2);

        // 多并行度下，需要根据关联条件进行 keyby，才能保证 key相同的数据到一起去，才能匹配上
        // ConnectedStreams -- keyBy --> 还是 ConnectedStreams
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyByCoStream
                = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);


        keyByCoStream
                .process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
                    Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
                    Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

                    /** 第一条流的处理逻辑 */
                    @Override
                    public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                        Integer id = value.f0;
                        // 1 s1 的数据来了，就存到变量中
                        if (!s1Cache.containsKey(id)) {
                            // 1-1 如果没有这个id的数据，就新建一个list
                            List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                            s1Values.add(value);
                            s1Cache.put(id, s1Values);
                        } else {
                            // 1-2 如果有这个id的数据，就把数据加到list中
                            s1Cache.get(id).add(value);
                        }

                        if (s2Cache.containsKey(id)) {
                            for (Tuple3<Integer, String, Integer> s2Value : s2Cache.get(id)) {
                                out.collect("processElement1():  s1:" + value + " -> " + s2Value);
                            }
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        Integer id = value.f0;
                        // 1 s1 的数据来了，就存到变量中
                        if (!s2Cache.containsKey(id)) {
                            // 1-1 如果没有这个id的数据，就新建一个list
                            List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                            s2Values.add(value);
                            s2Cache.put(id, s2Values);
                        } else {
                            // 1-2 如果有这个id的数据，就把数据加到list中
                            s2Cache.get(id).add(value);
                        }

                        if (s1Cache.containsKey(id)) {
                            for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                                out.collect("processElement2():  s1:" + s1Element + "<========>" + "s2:" + value);
                            }
                        }
                    }
                }).print();
        env.execute();
    }
}