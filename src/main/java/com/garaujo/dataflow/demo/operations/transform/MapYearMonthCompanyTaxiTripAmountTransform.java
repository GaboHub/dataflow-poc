package com.garaujo.dataflow.demo.operations.transform;

import com.garaujo.dataflow.demo.operations.dofns.YearMonthCompanyAmountOfTripsFn;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyCount;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class MapYearMonthCompanyTaxiTripAmountTransform extends PTransform<PCollection<KV<TaxiTripByYearMonthCompanyKey, Long>>, PCollection<TaxiTripByYearMonthCompanyCount>> {

    private static final long serialVersionUID = 1480781142574136212L;

    @Override
    public PCollection<TaxiTripByYearMonthCompanyCount> expand(PCollection<KV<TaxiTripByYearMonthCompanyKey, Long>> input) {
        return input.apply(ParDo.of(new YearMonthCompanyAmountOfTripsFn()));
    }


}
