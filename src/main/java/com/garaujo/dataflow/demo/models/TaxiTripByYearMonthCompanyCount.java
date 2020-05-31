package com.garaujo.dataflow.demo.models;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Builder
@Getter
@EqualsAndHashCode
public class TaxiTripByYearMonthCompanyCount implements Serializable {

    private static final long serialVersionUID = -2891595082085574237L;

    private final TaxiTripByYearMonthCompanyKey taxiTripByYearMonthCompanyKey;
    private final long amountOfTrips;

}
