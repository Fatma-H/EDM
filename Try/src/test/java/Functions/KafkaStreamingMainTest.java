package Functions;

    import com.typesafe.config.ConfigFactory;
    import lombok.SneakyThrows;
    import lombok.extern.slf4j.Slf4j;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.FileStatus;
    import org.apache.hadoop.fs.FileSystem;
    import org.apache.hadoop.fs.Path;
    import org.example.KafkaStreamingMain;
    import org.example.functions.reader.HdfsTextFileReader;
    import org.junit.Test;

    import java.util.Arrays;
    import java.util.List;
    import java.util.stream.Collectors;
    import java.util.stream.Stream;

    import static org.assertj.core.api.Assertions.assertThat;
@Slf4j
public class KafkaStreamingMainTest {

    @SneakyThrows
        @Test
        public void testMain() {



        String outputPathStr = ConfigFactory.load("applitest.conf").getString("3il.data.output");

                    FileSystem localFs = FileSystem.getLocal(new Configuration());
                    log.info("this is runned with the following filesystem:", localFs.getScheme());
                    KafkaStreamingMain.main(new String[0]);
                    Path outputPath = new Path(outputPathStr);
                    Stream<Path> jsonFilePaths = Arrays.stream(localFs.listStatus(outputPath)).map(FileStatus::getPath).filter(p -> p.getName().startsWith("part-") && p.toString().endsWith(".csv"));
                    List<String> lines = jsonFilePaths.flatMap(outputJsonFilePath -> new HdfsTextFileReader(localFs, outputJsonFilePath).get()).collect(Collectors.toList());
                    assertThat(lines).isNotEmpty().hasSize(1).contains("{\"date_signature\":\"28/09/2018\",\"id_acv\":\"acv-019\",\"insee_com\":\"89024\",\"lib_acv\":\"Auxerre\",\"lib_com\":\"Auxerre\"}");
                }
            }


