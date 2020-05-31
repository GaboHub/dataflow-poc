package com.garaujo.dataflow.demo.operations.transform;

import com.garaujo.dataflow.demo.models.OutputYearMonth;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyCount;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import com.garaujo.dataflow.demo.models.AmountOfTripsByYearMonthCompanyPaymentType;
import com.garaujo.dataflow.demo.operations.dofns.JointCountsDoFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class CountsJoinTransform {

    private CountsJoinTransform() {
        throw new IllegalArgumentException();
    }

    public static PCollection<OutputYearMonth> getFlightUpdateFullJoin(PCollection<TaxiTripByYearMonthCompanyCount> tripsbyYearMontyCompany,
                                                                       PCollection<AmountOfTripsByYearMonthCompanyPaymentType> countYearMonthCompanyPaymentType) {

        final TupleTag<TaxiTripByYearMonthCompanyCount> tripsByYearMonthCompanyTag = new TupleTag<TaxiTripByYearMonthCompanyCount>() {
            private static final long serialVersionUID = 2845427841964099965L;
        };
        final TupleTag<AmountOfTripsByYearMonthCompanyPaymentType> tripsByYearMonthCompanyPaymentTypeTag = new TupleTag<AmountOfTripsByYearMonthCompanyPaymentType>() {
            private static final long serialVersionUID = 5815510816343788882L;
        };

        PCollection<KV<TaxiTripByYearMonthCompanyKey, TaxiTripByYearMonthCompanyCount>> tripByYearMonthCompanyKey = tripsbyYearMontyCompany
                .apply("Generating TaxiTripByYearMonthCompanyCount key to be able to join collections", ParDo.of(new TripByYearMonthCompanyKey()));

        PCollection<KV<TaxiTripByYearMonthCompanyKey, AmountOfTripsByYearMonthCompanyPaymentType>> tripByYearMonthCompanyPaymentType = countYearMonthCompanyPaymentType
                .apply("Generating AmountOfTripsByYearMonthCompanyPaymentType key to be able to join collections", ParDo.of(new TripByYearMonthCompanyPaymentTypeKey()));

        PCollection<KV<TaxiTripByYearMonthCompanyKey, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
                .of(tripsByYearMonthCompanyTag, tripByYearMonthCompanyKey)
                .and(tripsByYearMonthCompanyPaymentTypeTag, tripByYearMonthCompanyPaymentType)
                .apply("FlightUpdateJoinTransform - Grouping by keys", CoGroupByKey.create());

        return kvpCollection.apply("Combining collections",
                ParDo.of(new JointCountsDoFn(tripsByYearMonthCompanyTag, tripsByYearMonthCompanyPaymentTypeTag)));
    }



    public static class TripByYearMonthCompanyKey extends DoFn<TaxiTripByYearMonthCompanyCount, KV<TaxiTripByYearMonthCompanyKey, TaxiTripByYearMonthCompanyCount>> {

        private static final long serialVersionUID = 2155848290954912261L;

        @ProcessElement
        public void processElement(ProcessContext c) {
            TaxiTripByYearMonthCompanyCount row = c.element();
            c.output(KV.of(row.getTaxiTripByYearMonthCompanyKey(), row));
        }
    }

    public static class TripByYearMonthCompanyPaymentTypeKey extends DoFn<AmountOfTripsByYearMonthCompanyPaymentType, KV<TaxiTripByYearMonthCompanyKey, AmountOfTripsByYearMonthCompanyPaymentType>> {

        private static final long serialVersionUID = 3886207103921386092L;

        @ProcessElement
        public void processElement(ProcessContext c) {
            AmountOfTripsByYearMonthCompanyPaymentType row = c.element();
            c.output(KV.of(row.getTaxiTripByYearMonthCompanyCount().getTaxiTripByYearMonthCompanyKey(), row));
        }
    }

}
