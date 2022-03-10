package com.asardaes.flink.watermarks;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

public class CustomTimestampAssigner implements TimestampAssigner<Pojo> {
    private final boolean delayed;

    public CustomTimestampAssigner(boolean delayed) {
        this.delayed = delayed;
    }

    @Override
    public long extractTimestamp(Pojo element, long recordTimestamp) {
        if (delayed) {
            return element.epoch - 1L;
        } else {
            return element.epoch;
        }
    }
}
