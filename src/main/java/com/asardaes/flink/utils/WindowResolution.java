package com.asardaes.flink.utils;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;

public class WindowResolution implements TemporalUnit, Serializable, Comparable<WindowResolution> {
    private static final long serialVersionUID = 1L;

    private final Duration duration;

    public WindowResolution(long seconds) {
        duration = Duration.ofSeconds(seconds);
    }

    @Override
    public Duration getDuration() {
        return duration;
    }

    @Override
    public boolean isDurationEstimated() {
        return false;
    }

    @Override
    public boolean isDateBased() {
        return false;
    }

    @Override
    public boolean isTimeBased() {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R extends Temporal> R addTo(R temporal, long amount) {
        return (R) temporal.plus(amount, this);
    }

    @Override
    public long between(Temporal temporal1Inclusive, Temporal temporal2Exclusive) {
        return temporal1Inclusive.until(temporal2Exclusive, this);
    }

    @Override
    public int compareTo(@NotNull WindowResolution o) {
        return this.duration.compareTo(o.duration);
    }

    @Override
    public String toString() {
        return "WindowResolution{" +
                "duration=" + duration +
                '}';
    }
}
