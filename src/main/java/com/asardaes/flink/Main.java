package com.asardaes.flink;

import com.asardaes.flink.dto.Pojo;
import com.asardaes.flink.input.TestInputFormat;
import com.asardaes.flink.operators.*;
import com.asardaes.flink.utils.WindowResolution;
import com.asardaes.flink.watermarks.CustomWatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.temporal.ChronoUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.getConfig().setAutoWatermarkInterval(1000L);

        DataStreamSource<Pojo> source = execEnv.createInput(new TestInputFormat(5000L))
                .setParallelism(1);

        SingleOutputStreamOperator<Pojo> start = source.process(new FilterWithSideOutput());
        FixedKeySelector keySelector = new FixedKeySelector();
        int windowSizeSeconds = 20;

        CustomWatermarkStrategy defaultWatermarkStrategy = new CustomWatermarkStrategy(new WindowResolution(windowSizeSeconds), windowSizeSeconds, false);
        DataStream<Pojo> stream2 = start.getSideOutput(FilterWithSideOutput.SIDE_OUTPUT)
                .keyBy(keySelector)
                .assignTimestampsAndWatermarks(defaultWatermarkStrategy)
                .name("stream2");

        DataStream<Pojo> stream1 = start
                .keyBy(keySelector)
                .assignTimestampsAndWatermarks(defaultWatermarkStrategy)
                .name("start-watermarked")
                .keyBy(keySelector)
                .window(
                        SlidingEventTimeWindows.of(
                                Time.seconds(windowSizeSeconds),
                                Time.seconds(1)
                        )
                )
                .process(new PojoWindowFunction())
                .name("start-windowed")
                .keyBy(keySelector)
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy(ChronoUnit.SECONDS, 5, true))
                .name("stream1");

        DataStreamUtils.reinterpretAsKeyedStream(stream1, keySelector)
                .intervalJoin(DataStreamUtils.reinterpretAsKeyedStream(stream2, keySelector))
                .between(Time.seconds(-1L * windowSizeSeconds), Time.milliseconds(0L))
                .lowerBoundExclusive()
                .process(new CustomProcessJoinFunction())
                .name("joined")
                .keyBy(keySelector)
                .window(
                        SlidingEventTimeWindows.of(
                                Time.seconds(windowSizeSeconds),
                                Time.seconds(1)
                        )
                )
                .process(new CustomProcessWindowFunction())
                .name("windowed")
                .addSink(new PrintSinkFunction<>());

        execEnv.execute();
    }
}
