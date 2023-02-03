package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.*;
import org.example.beans.Insee;
import org.example.functions.parsers.DataSetMapper;
import org.example.functions.reader.DsReader;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

//import org.example.Function.Reader.DataSetRowReader;
//import org.example.Function.Writers.DataSetRowWriter;
@Slf4j
public class Main {
    private static DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static void main(String[] args) throws IOException {
       // String appID = formatter.format(LocalDateTime.now()) + "_" + new Random().nextLong();
        //FileSystem hdfs = FileSystem.get(new Configuration());

        Config config = ConfigFactory.load("app.conf");
        String masterURL = config.getString("3il.master");
        String appName = config.getString("3il.appname");
        SparkSession spark = SparkSession.builder().master(masterURL).appName(appName).getOrCreate();
        FileSystem hdfs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        Path inputPath = new Path(config.getString("3il.data.input"));
        Path outputPath = new Path(config.getString("3il.data.output"));
         DsReader r = new DsReader(config.getString("3il.data.input"),spark,hdfs);
         Dataset<Row> ds =r.get();
         Dataset<Insee> cleanDS = new DataSetMapper().apply(ds);
        cleanDS.printSchema();
        cleanDS.show(5, false);
        //String keyToRegroup = "lib_com";
        // InseeStatFunction inseeStatFunction = new InseeStatFunction(keyToRegroup) ;
        //Dataset<Tuple2<String, Stat>> tuple2Dataset = inseeStatFunction.apply(ds);
        /******HADOOP*******

        String appID = formatter.format(LocalDateTime.now()) + "_" + new Random().nextLong();
        //FileSystem hdfs = FileSystem.get(new Configuration());
        FileSystem hdfs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

        /**DATA PROCESSING*
        log.info("Listing files from inputPath={} ...", inputPath);
        RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(inputPath, true);
        while (fileIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileIterator.next();
            FSDataInputStream instream = hdfs.open(locatedFileStatus.getPath());
            StringWriter writer = new StringWriter();
            IOUtils.copy(instream, writer, "UTF-8");
            Stream<String> lines = Arrays.stream(writer.toString().split("\n")).filter(l -> !l.startsWith("\"")); // remove header
            Map<String, Integer> wordCount = lines.flatMap(l -> Arrays.stream(l.split(";"))).filter(x -> !NumberUtils.isNumber(x)).map(x -> new Tuple2<>(x, 1)).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, Integer::sum));
            log.info("wordCount");
            wordCount.forEach((k, v) -> log.info("({} -> {})", k, v));
            String outlines = wordCount.entrySet().stream().map(e -> String.format("%s,%d", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
            FSDataOutputStream outstream = hdfs.create(outputPath.suffix(String.format("/wordcount/%s", locatedFileStatus.getPath().getName())));
            IOUtils.write(outlines, outstream, "UTF-8");
            outstream.close();
            instream.close();

        }
        DATA SAVING

        log.info("Copying files from inputPath={} to outputPath={}...",
                inputPath, outputPath);
        FileUtil.copy(
                FileSystem.get(inputPath.toUri(), hdfs.getConf()),
                inputPath,
                FileSystem.get(outputPath.toUri(), hdfs.getConf()),
                outputPath,
                false,
                hdfs.getConf());*/

    }
}

        //cleanDS.coalesce(2).write().mode(SaveMode.Overwrite).partitionBy("insee_com").json(outputPath)