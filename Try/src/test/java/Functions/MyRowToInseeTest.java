package Functions;

import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.beans.Insee;
import org.example.functions.parsers.RowToInsee;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class MyRowToInseeTest {
    @Test
    public void test(){
        StructType schema = new StructType(
                new StructField[] {
                        new StructField(
                                "insee_com",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField("lib_com",
                             DataTypes.StringType,
                            true,
                            Metadata.empty()),

                    new StructField("id_acv",
                            DataTypes.StringType,
                            true,
                            Metadata.empty()),
                    new StructField("lib_acv",
                            DataTypes.StringType,
                            true,
                            Metadata.empty()),
                    new StructField(
                            "date_signature",
                            DataTypes.StringType,
                            true,
                            Metadata.empty()
                    )}
                    );
                RowToInsee ri= new RowToInsee();
                String[] valeurs= new String[]{ "a","b","c","d","e"};
                Insee actual = ri.apply(new GenericRowWithSchema(valeurs,schema));
                Insee expected= Insee.builder()
                        .lib_com("b")
                        .lib_acv("d")
                        .insee_com("a")
                        .id_acv("c")
                        .date_signature("e")
                        .build();
        assertThat(actual).isEqualTo(expected);

                }



    }

