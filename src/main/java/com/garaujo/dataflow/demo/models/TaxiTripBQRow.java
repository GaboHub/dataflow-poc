package com.garaujo.dataflow.demo.models;

import com.garaujo.dataflow.demo.models.enums.PaymentType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.time.LocalDateTime;

@Getter
@EqualsAndHashCode
@ToString
public class TaxiTripBQRow implements Serializable {

    private static final long serialVersionUID = 5304271924481431364L;

    private final LocalDateTime tripStart;

    private final LocalDateTime tripEnd;

    private final PaymentType paymentType;

    private final String company;

    @Builder
    public TaxiTripBQRow(LocalDateTime tripStart, LocalDateTime tripEnd, PaymentType paymentType, String company) {
        this.tripStart = tripStart;
        this.tripEnd = tripEnd;
        this.paymentType = paymentType;
        this.company = company;
    }
}
