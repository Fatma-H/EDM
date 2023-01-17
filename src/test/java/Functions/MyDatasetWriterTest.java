package Functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;

import org.example.beans.Insee;
import org.example.functions.writers.Writer;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
public class MyDatasetWriterTest {
    @Test
    public void test() throws IOException {

        SparkSession spark = SparkSession.builder()
                .appName("MyDatasetReaderTest")
                .master("local[2]")
                .getOrCreate();
        Dataset<Insee> TestDs = spark.createDataset(
                Collections.singletonList(Insee.builder()
                        .insee_com("2408")
                        .lib_com("Laon")
                        .id_acv("acv-107")
                        .lib_acv("Laon")
                        .date_signature("28/09/2018")
                        .build()),Encoders.bean(Insee.class)

        );
        TestDs.show();

        Config config = ConfigFactory.load("app.conf") ;
        String outputPath = config.getString("3il.data.output") ;

        Writer W = new Writer(outputPath) ;

        W.accept(TestDs);


        Path output = Paths.get(outputPath);
        Stream<Path> jsonFilePaths = Files.list(output)
                .filter(p -> p.getFileName().toString().startsWith("part-") && p.toString().endsWith(".csv"))
                ;
        List<String> lines=jsonFilePaths
                .flatMap(
                        outputJsonfilepath ->{
                            Stream<String> jsonFileContent= Stream.empty();
                            try {
                                jsonFileContent=Files.lines(outputJsonfilepath);
                            }
                            catch (IOException e)
                            {
                                log.info("ccc");
                            }
                            return jsonFileContent;

                        }
                )
                .collect(Collectors.toList());
        assertThat(lines)
                .isNotEmpty()
                .contains("{\"date_signature\":\"28/09/2018\",\"id_acv\":\"acv-107\",\"insee_com\":\"2408\",\"lib_acv\":\"Laon\",\"lib_com\":\"Laon\"}")

        ;

    }


}

