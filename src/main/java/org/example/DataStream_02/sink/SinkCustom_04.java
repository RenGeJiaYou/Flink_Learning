package org.example.DataStream_02.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

/**
 * 自定义 Sink 类
 * 实际开发用得不多，只要知道实现一个 RichSinkFunction 的子类，并重写 open()/close()/invoke() 即可
 * 其中 invoke() 是核心逻辑，每条数据来都会调用一次，所以连接的实体类不要在 invoke() 里创建
 *
 * @author Island_World
 */

public class SinkCustom_04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("hadoop102", 7777);

        sensorDS.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<String>{
        Connection conn = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在这里 创建连接
            // conn = new xxxx
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 做一些清理，销毁连接
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // 写出逻辑
            // 这个方法是来一条数据，调用一次,所以不要在这里创建 连接对象
        }
    }
}
