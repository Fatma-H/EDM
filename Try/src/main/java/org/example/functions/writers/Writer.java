package org.example.functions.writers;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.example.beans.Insee;

import java.io.Serializable;
import java.util.function.Consumer;
@RequiredArgsConstructor
public class Writer implements Consumer<Dataset<Insee>>, Serializable {
    private final String output;
    @Override
    public void accept(Dataset<Insee> rowDataset) {

        rowDataset.write().mode(SaveMode.Overwrite).csv(output);
    }
}
