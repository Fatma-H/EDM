package org.example.functions.writers;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Time;
import org.example.beans.Insee;

import java.io.Serializable;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

@Slf4j
@AllArgsConstructor
public class InseeRDDProcessor implements VoidFunction2<JavaRDD<Insee>, Time> {
    private final SparkSession sparkSession;
    private final String outputPathStr;

    @Override
    public void call(JavaRDD<Insee> inseeJavaRDD, Time time) throws Exception {
        long ts = System.currentTimeMillis();
        log.info("micro-batch time={} at stored in folder={}", time, ts);
        log.info("micro-batch stored in folder={}",ts);

       // if(inseeJavaRDD.isEmpty()){
          //  log.info("no data found!");
          //  return;
       // }

        log.info("data under processing...");

        Dataset<Insee> InseeDataset = sparkSession.createDataset(
                inseeJavaRDD.rdd(),
                Encoders.bean(Insee.class)
        ).cache();


        InseeDataset.printSchema();
        InseeDataset.show(5, false);

        log.info("nb actesDeces = {}", InseeDataset.count());


       Writer writer = new Writer(outputPathStr + "/time=" + ts);
        writer.accept(InseeDataset);

            Dataset<Row> statds=InseeDataset.groupBy("lib_com").agg(count("lib_acv").as("nb"));
            statds.show(20,true);

            InseeDataset.show(5,false);

        InseeDataset.unpersist();
        log.info("done");
    }
}