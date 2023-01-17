package org.example.beans;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
@Getter
@Data
@Builder
public class Stat implements Serializable {

    private int count= 0;


}
