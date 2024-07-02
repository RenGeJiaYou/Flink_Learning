package org.example.DataStream_02.split;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.Functions.WaterSensorMapFunction;
import org.example.pojo.WaterSensor;

/**
 * 旁路分流：调用 sideOutput() 将一条 DataStream 拆分为多条。这种方式只处理一次数据，效率高。<p>
 * 旁路输出数据流的元素的数据类型可以与上游敛据流不同，多个旁路输出数据流之间，数据类型也不必相同。<p>
 * 当使用旁路输出的时候，首先需要定义 OutputTag， OutputTas 是每一个下游分支的标识。<p>
 * 定义好 OutputTag 之后，只有在特定的函数中才能使用旁路输出，例如 ProcessFunction、CoProcessFunction、KeyedProcessFunction 等。<p>
 *
 * @author Island_World
 */

public class side_output_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> ds = source.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    ctx.output(s1, value);
                } else if ("s2".equals(value.getId())) {
                    ctx.output(s2, value);
                } else {
                    // 主流
                    out.collect(value);
                }
            }
        });

        ds.print("主流，非s1,s2的传感器");

        ds.getSideOutput(s1).printToErr("s1传感器");
        ds.getSideOutput(s2).printToErr("s2传感器");

        env.execute();
    }
}
/**
 * socket 输入数据：
 * s1,1,1
 * s2,2,2
 * s1,3,46
 * s2,5,41
 * s8,4,4

 * 输出结果：
 * s1传感器:4> WaterSensor(id=s1, ts=1, vc=1)
 * s2传感器:5> WaterSensor(id=s2, ts=2, vc=2)
 * s1传感器:6> WaterSensor(id=s1, ts=3, vc=46)
 * s2传感器:7> WaterSensor(id=s2, ts=5, vc=41)
 * 主流，非s1,s2的传感器:8> WaterSensor(id=s8, ts=4, vc=4)
 * */
