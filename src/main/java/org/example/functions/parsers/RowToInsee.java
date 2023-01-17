package org.example.functions.parsers;

import org.apache.spark.sql.Row;
import org.example.beans.Insee;

import java.io.Serializable;
import java.util.function.Function;

public class RowToInsee implements Function<Row, Insee>, Serializable {
    @Override
    public Insee apply(Row row) {
        String insee_com = row.getAs("insee_com");
        String lib_com = row.getAs("lib_com");
        String id_acv = row.getAs("id_acv");
        String lib_acv = row.getAs("lib_acv");
        String date_signature = row.getAs("date_signature");

        return Insee.builder()
                .insee_com(insee_com)
                .lib_com(lib_com)
                .id_acv(id_acv)
                .lib_acv(lib_acv)
                .date_signature(date_signature)
                .build();
    }
}
