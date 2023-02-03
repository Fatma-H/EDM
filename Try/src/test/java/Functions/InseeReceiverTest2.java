package Functions;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.eclipse.jetty.server.RequestLog;
import org.example.beans.Insee;
import org.example.functions.reader.InseeReceiver;
import org.example.functions.writers.Writer;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.function.Supplier;
import java.util.Arrays;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;




import org.junit.Test;

import java.util.function.Supplier;

public class InseeReceiverTest2 {

    @Test
    public void InseeReceiverTest2() {

        SparkSession spark = SparkSession.builder()
                .appName("MyDatasetReaderTest")
                .master("local[2]")
                .getOrCreate();
        String path = "src/test/resources/data/input/liste-acv-com2022-20221118.csv";
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(5000));
        InseeReceiver inseeReceiver = new InseeReceiver(javaStreamingContext, path);
        Supplier<JavaDStream<Insee>> inseeSupplier = inseeReceiver;
        JavaDStream<Insee> inseeStream = inseeSupplier.get();

        // test that the inseeStream is not null
        assertNotNull(inseeStream);
    }
}


