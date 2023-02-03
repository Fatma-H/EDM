package Functions;
import lombok.SneakyThrows;
import org.example.beans.Insee;
import org.example.functions.writers.InseeRDDProcessor;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Time;


import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class InseeRDDProcessorTest {

    private SparkSession sparkSession;
    private JavaSparkContext sc;

    // Sample data to be used in the test
    private List<Insee> inseeList = Arrays.asList(
          new  Insee("John Doe", "Paris","dd","dddd","gg"),
          new  Insee("John Doe", "Paris","dd","ddddggdd","dsg"),
           new Insee("John Doe", "Paris","dd","dddsssd","xdx")
    );

    @Before
    public void setUp() {
        sparkSession = SparkSession.builder().appName("InseeRDDProcessorTest").master("local[*]").getOrCreate();
        sc = new JavaSparkContext(sparkSession.sparkContext());

    }

    @SneakyThrows
    @Test
    public void testCallMethod() {
        JavaRDD<Insee> inseeJavaRDD = sc.parallelize(inseeList);
        Time time = new Time(System.currentTimeMillis());
        InseeRDDProcessor processor = new InseeRDDProcessor(sparkSession, "/tmp/output");

        processor.call(inseeJavaRDD, time);

        Dataset<Insee> InseeDataset = sparkSession.createDataset(
                inseeJavaRDD.rdd(),
                Encoders.bean(Insee.class)
        ).cache();

        // Verify that the dataset has the same number of elements as the input data
        assertEquals(inseeList.size(), InseeDataset.count());
    }
}

