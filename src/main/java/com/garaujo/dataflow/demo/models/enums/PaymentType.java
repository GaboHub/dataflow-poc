package com.garaujo.dataflow.demo.models.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@AllArgsConstructor
@Getter
public enum PaymentType {

    NO_CHARGE(0, "No Charge"),
    CASH(1, "Cash"),
    CREDIT_CARD(2, "Credit Card"),
    UNKNOWN(3,"Unknown")
    ;

    private int id;
    private String type;

    public static PaymentType getPaymentType(String type) {
        return Arrays.stream(values()).filter(paymentType -> paymentType.getType().equals(type))
                .findAny().orElse(PaymentType.UNKNOWN);
    }

}
