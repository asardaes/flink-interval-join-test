package com.asardaes.flink.dto;

public class Pojo {
    public long epoch;
    public String string;
    public boolean main;

    private Pojo() {
    }

    public Pojo(long epoch, String string, boolean main) {
        this.epoch = epoch;
        this.string = string;
        this.main = main;
    }

    @Override
    public String toString() {
        return "Pojo{" +
                "epoch=" + epoch +
                ", string='" + string + '\'' +
                ", main=" + main +
                '}';
    }
}
