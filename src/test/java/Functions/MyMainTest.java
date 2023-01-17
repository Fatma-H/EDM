package Functions;

import com.typesafe.config.ConfigFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.Main;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RequiredArgsConstructor
@Slf4j
public class MyMainTest {
    private final String outputpath = ConfigFactory.load("applitest.conf").getString("3il.data.output");
    @Test
    public void test() throws IOException {
        Main.main(new String[0]);
        Path output = Paths.get(outputpath);
        Stream<Path> jsonFilePaths = Files.list(output)
                .filter(p -> p.getFileName().toString().startsWith("part-") && p.toString().endsWith(".json"))
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
                .contains("{\"date_signature\":\"28/09/2018\",\"id_acv\":\"acv-019\",\"insee_com\":\"89024\",\"lib_acv\":\"Auxerre\",\"lib_com\":\"Auxerre\"}")
        ;

    }
}


