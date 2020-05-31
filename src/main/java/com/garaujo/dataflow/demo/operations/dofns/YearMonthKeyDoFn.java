package com.garaujo.dataflow.demo.operations.dofns;

import com.garaujo.dataflow.demo.models.TaxiTripBQRow;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class YearMonthKeyDoFn extends DoFn<TaxiTripBQRow, KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow>> {

    private static final long serialVersionUID = -2484250535608817537L;

    @ProcessElement
    public void processElement(ProcessContext c) {
    TaxiTripBQRow row = c.element();
        c.output(KV.of(TaxiTripByYearMonthCompanyKey.builder()
                    .year(row.getTripStart().getYear())
                    .month(row.getTripStart().getMonth().getValue())
                    .company(row.getCompany())
                .build(), row));
    }
}