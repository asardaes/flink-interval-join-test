package com.asardaes.flink.operators;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class CustomProcessWindowFunction extends ProcessWindowFunction<Pojo, String, String, TimeWindow> implements ResultTypeQueryable<String> {
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public void process(String s, ProcessWindowFunction<Pojo, String, String, TimeWindow>.Context context, Iterable<Pojo> elements, Collector<String> out) {
        int count = 0;
        for (Pojo ignored : elements) {
            count++;
        }
        out.collect(count + " at " + Instant.ofEpochMilli(context.window().getEnd()));
    }
}
