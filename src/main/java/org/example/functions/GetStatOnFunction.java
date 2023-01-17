package org.example.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.example.beans.Insee;
import org.example.beans.Stat;

public class GetStatOnFunction implements MapFunction<Insee, Stat> {
    @Override
    public Stat call(Insee insee) throws Exception {

        return  Stat.builder()
                .count(1)
                .build();
    }
}
