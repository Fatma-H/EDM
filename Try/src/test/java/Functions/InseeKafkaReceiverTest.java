package Functions;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.spark.streaming.Duration;
import org.example.beans.Insee;
import org.example.functions.reader.InseeKafkaReciever;
import org.junit.Before;
import org.junit.Test;


import java.util.Arrays;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;

public class InseeKafkaReceiverTest {

    private InseeKafkaReciever receiver;
    private JavaStreamingContext jsc;

    @Before
    public void setUp() {
        jsc = new JavaStreamingContext("local[2]", "InseeKafkaReceiverTest", Duration.apply(1000));
        receiver = new InseeKafkaReciever(Arrays.asList("test"), jsc);
    }

    @Test
    public void testGet() {
        JavaDStream<Insee> result = receiver.get();
        assertNotNull(result);
        assertTrue(result instanceof JavaDStream);
    }

    @After
    public void tearDown() {
        jsc.stop(false);
    }

}
