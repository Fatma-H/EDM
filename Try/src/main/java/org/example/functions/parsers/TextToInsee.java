package org.example.functions.parsers;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.example.beans.Insee;

import java.io.Serializable;

public class TextToInsee implements Function<String, Insee> {
    @Override
    public Insee call(String line) {

String[] lines= StringUtils.splitByWholeSeparator(line,";");
        String insee_com = lines[0];
        String lib_com =lines[1];
        String id_acv =lines[2];
        String lib_acv =lines[3];
        String date_signature=lines[4] ;


        return Insee.builder()
                .insee_com(insee_com)
                .lib_com(lib_com)
                .id_acv(id_acv)
                .lib_acv(lib_acv)
                .date_signature(date_signature)
                .build();
    }
}