package Functions;

import org.example.functions.reader.DsRowReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class MyDatasetReaderTest {

    @Test
    public void testRead() {
        SparkSession spark = SparkSession.builder()
                .appName("MyDatasetReaderTest")
                .master("local[2]")
                .getOrCreate();
        String path = "src/test/resources/data/input/liste-acv-com2022-20221118.csv";
        DsRowReader reader = new DsRowReader(path,spark);

        Dataset<Row> dataset = reader.get();
        assertNotNull(dataset);
        assertEquals(235, dataset.count());
    }
}

