package com.garaujo.dataflow.demo.models;

import com.garaujo.dataflow.demo.models.enums.PaymentType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Builder
@Getter
@EqualsAndHashCode
public class AmountOfTripsByYearMonthCompanyPaymentType implements Serializable {

    private static final long serialVersionUID = 6286934263962661617L;
    private final TaxiTripByYearMonthCompanyCount taxiTripByYearMonthCompanyCount;
    private final PaymentType paymentType;
}
