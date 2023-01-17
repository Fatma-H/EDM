package org.example;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.*;
import org.example.beans.Insee;
import org.example.beans.Stat;
import org.example.functions.InseeStatFunction;
import org.example.functions.parsers.DataSetMapper;
import org.example.functions.reader.DsRowReader;
import org.example.functions.writers.Writer;
import scala.Tuple2;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

//import org.example.Function.Reader.DataSetRowReader;
//import org.example.Function.Writers.DataSetRowWriter;
public class Main {
    public static void main(String[] args) {

        Config config = ConfigFactory.load("app.conf");
        String masterURL= config.getString("3il.master");
        String appName = config.getString("3il.appname");
        SparkSession spark = SparkSession.builder().master(masterURL).appName(appName).getOrCreate();
        String inputPath = config.getString("3il.data.input");
        String outputPath = config.getString("3il.data.output");
        DsRowReader r = new DsRowReader(inputPath,spark);
        Dataset<Row> ds =r.get();
        Dataset<Insee> cleanDS = new DataSetMapper().apply(ds);
        cleanDS.printSchema();
        cleanDS.show(5, false);
        //String keyToRegroup = "lib_com";
       // InseeStatFunction inseeStatFunction = new InseeStatFunction(keyToRegroup) ;
        //Dataset<Tuple2<String, Stat>> tuple2Dataset = inseeStatFunction.apply(ds);
    }
}

        //cleanDS.coalesce(2).write().mode(SaveMode.Overwrite).partitionBy("insee_com").json(outputPath)