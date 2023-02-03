package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.beans.Insee;
import org.example.functions.reader.InseeReceiver;
import org.example.functions.writers.InseeRDDProcessor;

import java.io.IOException;

@Slf4j
public class StreamingMain {

    @SneakyThrows
    public static void main(String[] args)  throws IOException {

        Config config = ConfigFactory.load("app.conf");
        String masterURL = config.getString("3il.master");
        String appName = config.getString("3il.appname");
        String ckp = config.getString("3il.data.checkpoint");
        SparkSession spark = SparkSession.builder().master(masterURL).appName(appName).getOrCreate();
        Path inputPath = new Path(config.getString("3il.data.input"));
        String pathString = config.getString("3il.data.output");



        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(ckp, () -> new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(10000)));
      jsc.checkpoint(ckp);
        InseeReceiver reciever = new InseeReceiver(jsc, config.getString("3il.data.input"));
        JavaDStream<Insee> inseeJavaDStream = reciever.get();
        InseeRDDProcessor inseeRDDProcessor = new InseeRDDProcessor (spark, pathString);
        inseeJavaDStream.foreachRDD(
            inseeRDDProcessor
        );
        jsc.start();
        jsc.awaitTermination();
    }
}

//hdfs://localhost:20112
// JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(ckp, () ->{ JavaStreamingContext jscrs =new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(1000*60));
//   jscrs.checkpoint(ckp);
//  return jscrs;
//  },
// spark.sparkContext().hadoopConfiguration()
// );