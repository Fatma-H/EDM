package org.example.functions;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.beans.Insee;
import org.example.beans.Stat;
import org.example.functions.parsers.DataSetMapper;
import scala.Tuple2;

import java.util.function.Function;


@AllArgsConstructor
@RequiredArgsConstructor
public class InseeStatFunction implements Function<Dataset<Row>, Dataset<Tuple2<String, Stat>>> {


    private String key ;



    public Dataset<Tuple2<String, Stat>> apply(Dataset<Row> rowDataset) {
        final Dataset<Insee> cleanDs = new DataSetMapper().apply(rowDataset) ;


        final InseeToKeyFunction  inseeToKeyFunction = new InseeToKeyFunction(key);

        final GetStatOnFunction getStatOnFunction = new GetStatOnFunction() ;

        final ReduceFunction reduceFunction = new ReduceFunction() ;


        Dataset<Tuple2<String, Stat>> tuple2Dataset = cleanDs.groupByKey(inseeToKeyFunction, Encoders.STRING()).mapValues(getStatOnFunction, Encoders.bean(Stat.class)).reduceGroups(reduceFunction);
        tuple2Dataset.show();
        tuple2Dataset.printSchema();
        return tuple2Dataset;
    }
}

 