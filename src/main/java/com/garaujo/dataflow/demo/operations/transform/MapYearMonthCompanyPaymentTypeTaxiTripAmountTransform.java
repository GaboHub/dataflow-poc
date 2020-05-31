package com.garaujo.dataflow.demo.operations.transform;

import com.garaujo.dataflow.demo.models.YearMonthCompanyPaymentTypeKey;
import com.garaujo.dataflow.demo.operations.dofns.YearMonthCompanyPaymentTypeAmountOfTripsFn;
import com.garaujo.dataflow.demo.models.AmountOfTripsByYearMonthCompanyPaymentType;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class MapYearMonthCompanyPaymentTypeTaxiTripAmountTransform extends PTransform<PCollection<KV<YearMonthCompanyPaymentTypeKey, Long>>,
        PCollection<AmountOfTripsByYearMonthCompanyPaymentType>> {

    private static final long serialVersionUID = 1480781142574136212L;

    @Override
    public PCollection<AmountOfTripsByYearMonthCompanyPaymentType> expand(PCollection<KV<YearMonthCompanyPaymentTypeKey, Long>> input) {
        return input.apply(ParDo.of(new YearMonthCompanyPaymentTypeAmountOfTripsFn()));
    }


}
