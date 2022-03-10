package com.asardaes.flink.operators;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FilterWithSideOutput extends ProcessFunction<Pojo, Pojo> implements ResultTypeQueryable<Pojo> {
    public static final OutputTag<Pojo> SIDE_OUTPUT = new OutputTag<>("dummy", TypeInformation.of(Pojo.class));

    @Override
    public TypeInformation<Pojo> getProducedType() {
        return TypeInformation.of(Pojo.class);
    }

    @Override
    public void processElement(Pojo value, ProcessFunction<Pojo, Pojo>.Context ctx, Collector<Pojo> out) {
        ctx.output(SIDE_OUTPUT, value);
        if (value.main) {
            out.collect(value);
        }
    }
}
