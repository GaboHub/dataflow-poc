package com.garaujo.dataflow.demo.models;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@NoArgsConstructor
@Getter
@EqualsAndHashCode
@DefaultCoder(AvroCoder.class)
public class TaxiTripByYearMonthCompanyKey implements Serializable {

    private static final long serialVersionUID = -1293149490743213990L;

    private int year;
    private int month;
    private String company;

    @Builder
    public TaxiTripByYearMonthCompanyKey(int year, int month, String company) {
        this.year = year;
        this.month = month;
        this.company = company;
    }
}
