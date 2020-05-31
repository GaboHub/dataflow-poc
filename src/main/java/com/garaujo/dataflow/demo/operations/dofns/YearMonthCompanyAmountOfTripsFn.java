package com.garaujo.dataflow.demo.operations.dofns;

import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyCount;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class YearMonthCompanyAmountOfTripsFn extends DoFn<KV<TaxiTripByYearMonthCompanyKey, Long>, TaxiTripByYearMonthCompanyCount> {

    private static final long serialVersionUID = -2484250535608817537L;

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<TaxiTripByYearMonthCompanyKey, Long> row = c.element();
        c.output(TaxiTripByYearMonthCompanyCount.builder()
                .taxiTripByYearMonthCompanyKey(row.getKey())
                .amountOfTrips(row.getValue())
         .build());
    }
}