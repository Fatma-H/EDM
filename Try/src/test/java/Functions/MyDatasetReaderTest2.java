package Functions;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.functions.reader.DsReader;
import org.example.functions.reader.DsRowReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class MyDatasetReaderTest2 {

    @SneakyThrows
    @Test
    public void testRead() {
        SparkSession spark = SparkSession.builder()
                .appName("MyDatasetReaderTest")
                .master("local[2]")
                .getOrCreate();
        String path = "src/test/resources/data/input/liste-acv-com2022-20221118.csv";

        FileSystem hdfs = FileSystem.getLocal(spark.sparkContext().hadoopConfiguration());


        DsReader reader = new DsReader(path,spark,hdfs);

        Dataset<Row> dataset = reader.get();
        assertNotNull(dataset);
        assertEquals(235, dataset.count());
    }
}

