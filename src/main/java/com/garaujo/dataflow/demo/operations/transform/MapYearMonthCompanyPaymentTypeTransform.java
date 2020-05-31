package com.garaujo.dataflow.demo.operations.transform;

import com.garaujo.dataflow.demo.models.YearMonthCompanyPaymentTypeKey;
import com.garaujo.dataflow.demo.operations.dofns.YearMonthCompanyPaymentKeyDoFn;
import com.garaujo.dataflow.demo.models.TaxiTripBQRow;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class MapYearMonthCompanyPaymentTypeTransform extends PTransform<PCollection<KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow>>,
        PCollection<KV<YearMonthCompanyPaymentTypeKey, TaxiTripBQRow>>> {

    private static final long serialVersionUID = 1480781142574136212L;

    @Override
    public PCollection<KV<YearMonthCompanyPaymentTypeKey, TaxiTripBQRow>>expand(PCollection<KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow>> input) {
        return input.apply(ParDo.of(new YearMonthCompanyPaymentKeyDoFn()));
    }


}
