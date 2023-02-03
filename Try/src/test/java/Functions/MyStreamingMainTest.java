package Functions;

import com.typesafe.config.ConfigFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.example.Main;
import org.example.StreamingMain;
import org.example.functions.reader.HdfsTextFileReader;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;


import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RequiredArgsConstructor
@Slf4j
public class MyStreamingMainTest {
    private final String outputpath = ConfigFactory.load("applitest.conf").getString("3il.data.output");
    @Test
    public void test() throws IOException {
        FileSystem localFs = FileSystem.getLocal(new Configuration());

        StreamingMain.main(new String[0]);


        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputpath);
        Stream<org.apache.hadoop.fs.Path> jsonFilePaths = Arrays.stream(localFs.listStatus(outputPath))
                .map(FileStatus::getPath)
                .filter(p -> p.getName().startsWith("part-") && p.toString().endsWith(".json"));

        List<String> lines = jsonFilePaths
                .flatMap(outputJsonFilePath -> new HdfsTextFileReader(localFs, outputJsonFilePath).get())
                .collect(Collectors.toList());

        assertThat(lines)
                .isNotEmpty()
                .contains("{\"date_signature\":\"28/09/2018\",\"id_acv\":\"acv-019\",\"insee_com\":\"89024\",\"lib_acv\":\"Auxerre\",\"lib_com\":\"Auxerre\"}")
        ;

    }
}


