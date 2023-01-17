package org.example.functions.reader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.function.Supplier;
@RequiredArgsConstructor
public class DsRowReader implements Supplier<Dataset<Row>>, Serializable {
    private final String input;
    private final SparkSession sparkSession;


    @Override
    public  Dataset<Row> get() {

        StructType schema =new StructType(
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
        Dataset<Row> ds = sparkSession.read().schema(schema).option("delimiter", ";").option("header", "true").csv(input);
        ds.printSchema();
        ds.show(5,false);
        return ds;}}
   