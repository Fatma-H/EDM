package org.example.functions.parsers;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.beans.Insee;

import java.io.Serializable;
import java.util.function.Function;
@RequiredArgsConstructor
public class DataSetMapper implements Function<Dataset<Row>, Dataset<Insee>>, Serializable {
    private final RowToInsee r = new RowToInsee();
    private final MapFunction<Row,Insee> F = r::apply;
    @Override
    public Dataset<Insee> apply(Dataset<Row> rowDataset) {
        return rowDataset.map(F, Encoders.bean(Insee.class));
    }
}
