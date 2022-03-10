package com.asardaes.flink.watermarks;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.common.eventtime.*;

import java.time.temporal.TemporalUnit;

public class CustomWatermarkStrategy implements WatermarkStrategy<Pojo> {
    private final TemporalUnit period;
    private final int windowSizeSeconds;
    private final boolean delayedTimestamp;

    public CustomWatermarkStrategy(TemporalUnit period, int windowSizeSeconds, boolean delayedTimestamp) {
        this.period = period;
        this.windowSizeSeconds = windowSizeSeconds;
        this.delayedTimestamp = delayedTimestamp;
    }

    @Override
    public WatermarkGenerator<Pojo> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomWatermarkGenerator<>(period, windowSizeSeconds);
    }

    @Override
    public TimestampAssigner<Pojo> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new CustomTimestampAssigner(delayedTimestamp);
    }
}
