package com.yjx.flink.read;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author zhang
 * @time 9/12/2025 下午5:20
 * @description
 */
public class ReadDorisByDataStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DorisOptions.Builder builder = DorisOptions.builder()
                .setTableIdentifier("scott.emp")
                .setFenodes("node01:8030")
                .setUsername("root")
                .setPassword("123456");
        DorisSource<List<?>> dorisSource = DorisSource.<List<?>>builder()
                .setDorisOptions(builder.build())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();

        DataStreamSource<List<?>> source = environment.fromSource(
                dorisSource,
                WatermarkStrategy.noWatermarks(),
                "DorisSource"
        );
        source.print();
        environment.execute();
    }
}
