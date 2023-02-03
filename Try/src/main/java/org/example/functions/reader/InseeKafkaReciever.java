package org.example.functions.reader;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.example.beans.Insee;
import org.example.functions.parsers.TextToInsee;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
@Slf4j
@RequiredArgsConstructor
public class InseeKafkaReciever  implements Supplier<JavaDStream<Insee>>, Serializable {
    private final List<String> topics;
    private final JavaStreamingContext jsc;
    private final TextToInsee toi= new TextToInsee();
    private final Map<String,Object> kafkaParams= new HashMap<String,Object>(){{
        put("bootstrap.servers","localhost:9092");
        put("key.deserializer", StringDeserializer.class);
        put("value.deserializer", StringDeserializer.class);
        put("group.id","spark-kafka-integ");
        put("auto.offset.reset","earliest");

    }};

    @Override
    public JavaDStream<Insee> get() {
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<Insee> map =(directStream.map(ConsumerRecord::value)).map(toi::call);



        return map;

    }




}
