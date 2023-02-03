package org.example.functions;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.beans.Insee;
import org.example.functions.writers.Writer;


@Slf4j
@AllArgsConstructor

public class InseeRDDProcessorold implements VoidFunction<JavaRDD<Insee>>  {
    private final SparkSession spark;
    private final String outputpath;



    @Override
    public void call(JavaRDD<Insee> inseeJavaRDD) throws Exception {
        log.info("micro batch is now at time {}", System.currentTimeMillis());
        Dataset<Insee> inseeDataset = spark.createDataset(inseeJavaRDD.rdd(), Encoders.bean(Insee.class));
        inseeDataset.cache();
        inseeDataset.printSchema();
        inseeDataset.show(5, false);
        log.info("nb de insee instances {}",inseeDataset.count());
        new Writer(outputpath + "\time" + System.currentTimeMillis()).accept(inseeDataset);
        log.info("ok");
    }
}
