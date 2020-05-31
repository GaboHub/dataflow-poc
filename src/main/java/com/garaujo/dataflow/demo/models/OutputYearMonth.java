package com.garaujo.dataflow.demo.models;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Builder
@Getter
@EqualsAndHashCode
public class OutputYearMonth implements Serializable {

    private static final long serialVersionUID = 9145801305694903187L;
    private final TaxiTripByYearMonthCompanyKey taxiTripByYearMonthCompanyKey;
    private final long totalTrips;
    private final long cashTrips;
    private final long noChargeTrips;
    private final long creditCardTrips;
    private final long unkwonPaymentTrips;


    @Override
    public String toString() {
        return taxiTripByYearMonthCompanyKey.getYear() + "," +
                taxiTripByYearMonthCompanyKey.getMonth() + "," +
                taxiTripByYearMonthCompanyKey.getCompany() + "," +
                totalTrips + "," +
                cashTrips + "," +
                noChargeTrips + "," +
                creditCardTrips + "," +
                unkwonPaymentTrips ;

    }
}
