package com.garaujo.dataflow.demo;

import com.garaujo.dataflow.demo.operations.transform.MapYearMonthCompanyPaymentTypeTransform;
import com.garaujo.dataflow.demo.config.ChicagoTripsOptions;
import com.garaujo.dataflow.demo.models.AmountOfTripsByYearMonthCompanyPaymentType;
import com.garaujo.dataflow.demo.models.OutputYearMonth;
import com.garaujo.dataflow.demo.models.TaxiTripBQRow;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyCount;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import com.garaujo.dataflow.demo.operations.extract.ExtractBQFacade;
import com.garaujo.dataflow.demo.operations.transform.CountsJoinTransform;
import com.garaujo.dataflow.demo.operations.transform.MapYearMonthCompanyPaymentTypeTaxiTripAmountTransform;
import com.garaujo.dataflow.demo.operations.transform.MapYearMonthCompanyTaxiTripAmountTransform;
import com.garaujo.dataflow.demo.operations.transform.MapYearMonthTaxiTripTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ChicagoTripsFacade {


    public void run(ChicagoTripsOptions chicagoTripsOptions) {

        Pipeline p = Pipeline.create(chicagoTripsOptions);

        PCollection<TaxiTripBQRow> taxiTripRow = ExtractBQFacade.getTaxiTripRow(p);

        PCollection<KV<TaxiTripByYearMonthCompanyKey, TaxiTripBQRow>> groupedByYearMonthCompany = taxiTripRow
                .apply(new MapYearMonthTaxiTripTransform());

        PCollection<TaxiTripByYearMonthCompanyCount> tripsbyYearMontyCompany = groupedByYearMonthCompany
                .apply("Count company Taxi Trip by year month", Count.perKey())
                .apply(new MapYearMonthCompanyTaxiTripAmountTransform());

        PCollection<AmountOfTripsByYearMonthCompanyPaymentType> countYearMonthCompanyPaymentType = groupedByYearMonthCompany
                .apply("Map payment type by year Month", new MapYearMonthCompanyPaymentTypeTransform())
                .apply(Count.perKey())
                .apply("Count payment by year month company and payment type", new MapYearMonthCompanyPaymentTypeTaxiTripAmountTransform());


        PCollection<OutputYearMonth> outputYearCollection = CountsJoinTransform.joinPCollections(tripsbyYearMontyCompany,
                                                                                    countYearMonthCompanyPaymentType);

        saveToStorage(outputYearCollection, chicagoTripsOptions);
        p.run();

    }

    private static void saveToStorage(PCollection<OutputYearMonth> output, ChicagoTripsOptions options) {
        output.apply(ToString.elements())
                    .apply("Save CSV output", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".csv"));
    }

}
