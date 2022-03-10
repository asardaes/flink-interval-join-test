package com.asardaes.flink.operators;

import com.asardaes.flink.dto.Pojo;
import org.apache.flink.api.java.functions.KeySelector;

public class FixedKeySelector implements KeySelector<Pojo, String> {
    @Override
    public String getKey(Pojo value) {
        return "key";
    }
}
