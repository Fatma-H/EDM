package Functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.util.Lists;
import org.example.beans.Insee;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MyStatsTest {
    @Test
    public void test(){
       /* StructType schema =new StructType(
                new StructField[]{
                        new StructField("insee_com",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "lib_com",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "id_acv",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "lib_acv",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "date_signature",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),

                }
        );
        SparkSession spark = SparkSession.builder()
                .appName("MyDatasetReaderTest")
                .master("local[2]")
                .getOrCreate();
        String[] valeurs= new String[]{ "a","b","c","d","e"};
        Dataset<Row> rows = spark.createDataset(Lists.newArrayList(new GenericRowWithSchema(valeurs,schema)));

        final Dataset<Row> actual = spark.createDataset(rows, schema);
        /*  // To create Dataset<Row> using SparkSession
   Dataset<Row> people = spark.read().parquet("...");
   Dataset<Row> department = spark.read().parquet("...");

   people.filter(people.col("age").gt(30))
     .join(department, people.col("deptId").equalTo(department.col("id")))
     .groupBy(department.col("name"), people.col("gender"))
     .agg(avg(people.col("salary")), max(people.col("age")));*/
    }
}
