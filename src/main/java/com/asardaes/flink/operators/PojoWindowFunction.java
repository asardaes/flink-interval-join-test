package com.asardaes.flink.operators;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class PojoWindowFunction extends ProcessWindowFunction<Pojo, Pojo, String, TimeWindow> implements ResultTypeQueryable<Pojo> {
    @Override
    public TypeInformation<Pojo> getProducedType() {
        return TypeInformation.of(Pojo.class);
    }

    @Override
    public void process(String s, ProcessWindowFunction<Pojo, Pojo, String, TimeWindow>.Context context, Iterable<Pojo> elements, Collector<Pojo> out) {
        if (System.currentTimeMillis() % 2 == 0) {
            Pojo sample = elements.iterator().next();
            sample.epoch = context.window().getEnd();
            out.collect(sample);
        }
    }
}
