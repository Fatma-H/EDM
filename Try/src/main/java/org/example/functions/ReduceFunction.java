package org.example.functions;


import org.example.beans.Stat;

public class ReduceFunction implements org.apache.spark.api.java.function.ReduceFunction<Stat> {
    @Override
    public Stat call(Stat stats, Stat t1) throws Exception {

        return Stat.builder()
                .count(stats.getCount() + t1.getCount())
                .build();
    }
}
