package org.example.DataStream_02;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Functions.MyFilterFunction;
import org.example.pojo.WaterSensor;

/**
 * 富函数.相较普通自定义函数，主要是多了
 * 1. open() / close() 生命周期方法，让你可以在该算子在开始执行前/结束执行前做点什么。一个算子的每个并行子任务都会执行一次 open()/close()
 * 2. 富函数的工作方法,如下面的 map 等,每来一条数据更新一次
 * 3. 提供了 RuntimeContext 类，可以获取运行时的一些信息，比如子任务的名称/序号等
 *
 * @author Island_World
 */

public class UDF_Rich_Func_05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 3L, 3),
                new WaterSensor("sensor_3", 3L, 3));

        // 1 先分组
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(s -> s.id);

        // 2 调用自定义的 FilterFuntion 类，注意该类是有参构造
        keyedStream.map(new RichMapFunction<WaterSensor, Object>() {
            @Override
            public Object map(WaterSensor value) throws Exception {
                return value.vc * 23;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext ctx = getRuntimeContext();
                System.out.println("子任务编号=" + ctx.getIndexOfThisSubtask() + ",子任务名称=" + ctx.getTaskNameWithSubtasks());
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close func finished");
            }
        }).print();


        env.execute();
    }
}
/*
* 输出结果：
*    子任务编号=0,子任务名称=Map -> Sink: Print to Std. Out (1/1)#0
*    23
*    46
*    69
*    69
*    close func finished
* */