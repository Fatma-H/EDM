package org.example.functions.reader;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.beans.Insee;
import org.example.functions.parsers.InseeFileInputFormat;
import org.example.functions.parsers.InseeLongWritable;
import org.example.functions.parsers.InseeText;
import org.example.functions.parsers.TextToInsee;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class InseeReceiver implements Supplier<JavaDStream<Insee>> {
    private final JavaStreamingContext javaStreamingContext;
    private final String inputPathStr;

    private final TextToInsee textToInseeFunc = new TextToInsee();
    private final Function<String,Insee> mapper = textToInseeFunc::call;
    private final Function<Path, Boolean> filter = p -> p.getName().endsWith(".txt");

    @Override
    public JavaDStream<Insee> get() {
        JavaPairInputDStream<InseeLongWritable, InseeText> inputDStream = javaStreamingContext
                .fileStream(
                        inputPathStr,
                        InseeLongWritable.class,
                        InseeText.class,
                        InseeFileInputFormat.class,
                        filter,
                        true
                );
        return inputDStream.map(t -> t._2().toString()).map(mapper);
    }
}
