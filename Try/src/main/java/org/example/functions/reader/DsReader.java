package org.example.functions.reader;

import lombok.RequiredArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class DsReader implements Supplier<Dataset<Row>>, Serializable {
    private final String input;
    private final SparkSession sparkSession;

    private final FileSystem hdfs;
    private Class<org.apache.spark.sql.Row> Row;

    @Override
    public  Dataset<Row> get() {
        try {
            Path path = new Path(input);
            if (hdfs.exists(path)) {
                FileStatus[] listFiles= hdfs.listStatus(path);
                String[] inputpaths =Arrays.stream(listFiles).filter(FileStatus::isFile).map(l->l.getPath().toString()).toArray(String[]::new );
                Dataset<Row> ds = sparkSession.read().option("delimiter", ";").option("header", "true").csv(inputpaths);
                ds.printSchema();
                ds.show(5, false);
                return ds;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

return sparkSession.emptyDataset(Encoders.bean(Row));
    }}