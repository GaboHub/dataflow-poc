package com.garaujo.dataflow.demo.models;

import com.garaujo.dataflow.demo.models.enums.PaymentType;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@NoArgsConstructor
@Getter
@DefaultCoder(AvroCoder.class)
public class YearMonthCompanyPaymentTypeKey implements Serializable {

    private static final long serialVersionUID = -7858878343145144192L;
    private TaxiTripByYearMonthCompanyKey taxiTripByYearMonthCompanyKey;
    private PaymentType paymentType;

    @Builder
    public YearMonthCompanyPaymentTypeKey(TaxiTripByYearMonthCompanyKey taxiTripByYearMonthCompanyKey, PaymentType paymentType) {
        this.taxiTripByYearMonthCompanyKey = TaxiTripByYearMonthCompanyKey.builder()
                    .month(taxiTripByYearMonthCompanyKey.getMonth())
                    .year(taxiTripByYearMonthCompanyKey.getYear())
                    .company(taxiTripByYearMonthCompanyKey.getCompany())
                .build();
        this.paymentType = paymentType;
    }
}
