package org.example.functions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;

import org.example.beans.Insee;


@AllArgsConstructor
@RequiredArgsConstructor
@Getter

public class InseeToKeyFunction implements MapFunction <Insee , String>{


    private String key ;


    @Override
    public String call(Insee insee) throws Exception {

        if (this.key.equals("insee_com")) {
            return insee.getInsee_com() ;
        }

        else if (this.key.equals("lib_com") ) {
            return insee.getLib_com();
        //}
        //else if (this.key.equals("id_acv") ) {
         //   return Insee.getId_acv() ;
        //}
       // else if (this.key.equals("lib_acv") ) {
          //  return Insee.getLib_acv() ;
        }

        return "";
    }
}
