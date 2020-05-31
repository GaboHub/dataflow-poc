package com.garaujo.dataflow.demo.operations.transform;

import com.garaujo.dataflow.demo.models.TaxiTripBQRow;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import com.garaujo.dataflow.demo.operations.dofns.YearMonthKeyDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class MapYearMonthTaxiTripTransform extends PTransform<PCollection<TaxiTripBQRow>, PCollection<KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow>>> {

    private static final long serialVersionUID = 1480781142574136212L;

    @Override
    public PCollection<KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow>> expand(PCollection<TaxiTripBQRow> input) {
        return input.apply(ParDo.of(new YearMonthKeyDoFn()));
    }


}
