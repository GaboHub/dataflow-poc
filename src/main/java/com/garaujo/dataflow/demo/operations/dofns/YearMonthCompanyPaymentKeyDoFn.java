package com.garaujo.dataflow.demo.operations.dofns;

import com.garaujo.dataflow.demo.models.TaxiTripBQRow;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import com.garaujo.dataflow.demo.models.YearMonthCompanyPaymentTypeKey;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class YearMonthCompanyPaymentKeyDoFn extends DoFn<KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow>, KV<YearMonthCompanyPaymentTypeKey, TaxiTripBQRow>> {

    private static final long serialVersionUID = -2484250535608817537L;

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow> row = c.element();

        c.output(KV.of(YearMonthCompanyPaymentTypeKey.builder()
                    .taxiTripByYearMonthCompanyKey(row.getKey())
                    .paymentType(row.getValue().getPaymentType())
                .build(), row.getValue()));
    }
}