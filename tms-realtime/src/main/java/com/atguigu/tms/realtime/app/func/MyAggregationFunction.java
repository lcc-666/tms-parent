package com.atguigu.tms.realtime.app.func;

import org.apache.flink.api.common.functions.AggregateFunction;

public abstract class MyAggregationFunction<T> implements AggregateFunction<T,T,T> {
    @Override
    public T createAccumulator() {
        return null;
    }

    @Override
    public T getResult(T accumulator) {
        return accumulator;
    }

    @Override
    public T merge(T a, T b) {
        return null;
    }
}

