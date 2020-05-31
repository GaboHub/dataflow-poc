package com.garaujo.dataflow.demo.operations.dofns;

import com.garaujo.dataflow.demo.models.YearMonthCompanyPaymentTypeKey;
import com.garaujo.dataflow.demo.models.AmountOfTripsByYearMonthCompanyPaymentType;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyCount;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class YearMonthCompanyPaymentTypeAmountOfTripsFn extends DoFn<KV<YearMonthCompanyPaymentTypeKey, Long>,
        AmountOfTripsByYearMonthCompanyPaymentType> {

    private static final long serialVersionUID = -2484250535608817537L;

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<YearMonthCompanyPaymentTypeKey, Long> row = c.element();
        c.output(AmountOfTripsByYearMonthCompanyPaymentType.builder()
                .taxiTripByYearMonthCompanyCount(TaxiTripByYearMonthCompanyCount.builder()
                        .taxiTripByYearMonthCompanyKey(row.getKey().getTaxiTripByYearMonthCompanyKey())
                        .amountOfTrips(row.getValue())
                .build())
                .paymentType(row.getKey().getPaymentType())
         .build());
    }
}