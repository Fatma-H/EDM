package org.example.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
@Data
@Getter
@Builder
@AllArgsConstructor
public class Insee implements Serializable {
    private String insee_com;
    private String lib_com;
    private String id_acv;
    private String lib_acv;
    private String date_signature;


}
