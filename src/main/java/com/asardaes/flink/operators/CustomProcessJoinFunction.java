package com.asardaes.flink.operators;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class CustomProcessJoinFunction extends ProcessJoinFunction<Pojo, Pojo, Pojo> implements ResultTypeQueryable<Pojo> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomProcessJoinFunction.class);

    @Override
    public TypeInformation<Pojo> getProducedType() {
        return TypeInformation.of(Pojo.class);
    }

    @Override
    public void processElement(Pojo left, Pojo right, ProcessJoinFunction<Pojo, Pojo, Pojo>.Context ctx, Collector<Pojo> out) {
        LOG.trace("Joining with left={}, right={}, combined={}", Instant.ofEpochMilli(ctx.getLeftTimestamp()), Instant.ofEpochMilli(ctx.getLeftTimestamp()), Instant.ofEpochMilli(ctx.getTimestamp()));
        out.collect(left);
    }
}
