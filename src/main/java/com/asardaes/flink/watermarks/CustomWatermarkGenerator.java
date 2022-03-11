package com.asardaes.flink.watermarks;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;

public class CustomWatermarkGenerator<T> implements WatermarkGenerator<T>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CustomWatermarkGenerator.class);

    private final TemporalUnit period;
    private final int windowSizeSeconds;

    private Instant maxEventTime = null;
    private Instant maxEventTimeTruncated = Instant.ofEpochMilli(0L);
    private Instant lastWatermarkTimeTruncated = null;
    private int forceAdvanceMultiplier = 1;

    public CustomWatermarkGenerator(TemporalUnit period, int windowSizeSeconds) {
        this.period = period;
        this.windowSizeSeconds = windowSizeSeconds;
    }

    @Override
    public synchronized void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        if (maxEventTime == null) {
            maxEventTime = Instant.ofEpochMilli(eventTimestamp);
        } else if (eventTimestamp > maxEventTime.toEpochMilli()) {
            maxEventTime = Instant.ofEpochMilli(eventTimestamp);
            forceAdvanceMultiplier = 1;
        } else if (forceAdvanceMultiplier == 1L) {
            // delay increasing forceAdvanceMultiplier if events are still coming in
            lastWatermarkTimeTruncated = Instant.now().truncatedTo(period);
        }
    }

    @Override
    public synchronized void onPeriodicEmit(WatermarkOutput output) {
        Instant now = Instant.now();
        Instant truncatedInstant = now.truncatedTo(period);

        if (maxEventTime == null) {
            output.markIdle();
            return;
        }

        if (Duration.between(maxEventTimeTruncated, maxEventTime).compareTo(period.getDuration()) >= 0) {
            emitEventDrivenWatermark(output, truncatedInstant);
            return;
        }
        // if I uncomment this, it does start printing after a while
//        else {
//            output.markIdle();
//        }

        if (truncatedInstant.compareTo(lastWatermarkTimeTruncated) > 0) {
            if (forceAdvanceMultiplier > windowSizeSeconds) {
                // stream has been idle for windowSize * period, don't emit more watermarks
                output.markIdle();
            } else {
                emitPeriodicWatermark(output, truncatedInstant);
            }
        }
    }

    // generate watermark when the newest event time is >= max (truncated) event time + slide time
    private void emitEventDrivenWatermark(WatermarkOutput output, Instant truncatedInstant) {
        lastWatermarkTimeTruncated = truncatedInstant;
        maxEventTimeTruncated = maxEventTime.truncatedTo(period);
        LOG.debug("Emitting event-driven watermark: {}", maxEventTime);
        output.emitWatermark(new Watermark(maxEventTime.toEpochMilli()));
    }

    // generate watermark every "slide" time if no new events arrive
    private void emitPeriodicWatermark(WatermarkOutput output, Instant truncatedInstant) {
        lastWatermarkTimeTruncated = truncatedInstant;
        Instant timeToForceAdvanceFlinkTime = this.maxEventTime
                .truncatedTo(period)
                .plus(period.getDuration().multipliedBy(forceAdvanceMultiplier++));

        LOG.debug("Emitting periodic watermark: {}", timeToForceAdvanceFlinkTime);
        output.emitWatermark(new Watermark(timeToForceAdvanceFlinkTime.toEpochMilli()));
    }
}
