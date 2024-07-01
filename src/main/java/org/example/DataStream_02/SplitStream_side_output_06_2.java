package org.example.DataStream_02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.Functions.MyFilterFunction;
import org.example.Functions.WaterSensorMapFunction;
import org.example.pojo.WaterSensor;

/**
 * 简单分流：调用 filter() 将一条 DataStream 拆分为多条。这种方式虽然简单，但同一份数据会被多次处理，效率低下。
 *
 * @author Island_World
 */

public class SplitStream_side_output_06_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        source.process(new ProcessFunction<WaterSensor, WaterSensor>() {
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
        })

        env.execute();
    }
}
