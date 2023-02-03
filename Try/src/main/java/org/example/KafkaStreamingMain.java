package org.example;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.beans.Insee;
import org.example.functions.reader.InseeKafkaReciever;
import org.example.functions.writers.InseeRDDProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

public class KafkaStreamingMain {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamingMain.class);

    public static void main(String[] args) {
        Config config = ConfigFactory.load("app.conf");
        String masterUrl = config.getString("3il.master");

        SparkConf conf = new SparkConf().setAppName("InseeKafkaStreaming").setMaster(masterUrl);
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));

        List<String> topics = config.getStringList("3il.data.key");
        InseeKafkaReciever receiver = new InseeKafkaReciever(topics, jsc);
        JavaDStream<Insee> inseeStream = receiver.get();
        InseeRDDProcessor inseeRDDProcessor= new InseeRDDProcessor(SparkSession.builder().getOrCreate(),config.getString("3il.data.output"));
        inseeStream.foreachRDD(inseeRDDProcessor);


        try {
            jsc.start();
            jsc.awaitTermination();
        } catch (Exception e) {
            logger.error("Error while processing Insee data from Kafka", e);
        } finally {
            jsc.stop();
        }
    }
}
