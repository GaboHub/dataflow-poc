package com.garaujo.dataflow.demo.operations.dofns;

import com.garaujo.dataflow.demo.models.AmountOfTripsByYearMonthCompanyPaymentType;
import com.garaujo.dataflow.demo.models.OutputYearMonth;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyCount;
import com.garaujo.dataflow.demo.models.TaxiTripByYearMonthCompanyKey;
import com.garaujo.dataflow.demo.models.enums.PaymentType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JointCountsDoFn extends DoFn<KV<TaxiTripByYearMonthCompanyKey, CoGbkResult>, OutputYearMonth> {

    private static final long serialVersionUID = 8047470343607818269L;

    final TupleTag<TaxiTripByYearMonthCompanyCount> tripsByYearMonthCompanyTag;
    final TupleTag<AmountOfTripsByYearMonthCompanyPaymentType> tripsByYearMonthCompanyPaymentTypeTag;

    public JointCountsDoFn(final TupleTag<TaxiTripByYearMonthCompanyCount> tripsByYearMonthCompanyTag,
                           final TupleTag<AmountOfTripsByYearMonthCompanyPaymentType> tripsByYearMonthCompanyPaymentTypeTag) {
        this.tripsByYearMonthCompanyTag = tripsByYearMonthCompanyTag;
        this.tripsByYearMonthCompanyPaymentTypeTag = tripsByYearMonthCompanyPaymentTypeTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<TaxiTripByYearMonthCompanyKey, CoGbkResult> e = c.element();

        TaxiTripByYearMonthCompanyKey tripByYearMonthCompanyKey = e.getKey();

        if (Objects.isNull(tripByYearMonthCompanyKey))
            return;

        TaxiTripByYearMonthCompanyCount classCompositionByFlight = e.getValue().getOnly(tripsByYearMonthCompanyTag, null);
        Iterable<AmountOfTripsByYearMonthCompanyPaymentType> tripsByMonthYearCompanyPaymentTypes = e.getValue().getAll(tripsByYearMonthCompanyPaymentTypeTag);

        if (Objects.isNull(classCompositionByFlight) || Objects.isNull(tripsByMonthYearCompanyPaymentTypes))
            return;

        Supplier<Stream<AmountOfTripsByYearMonthCompanyPaymentType>> tripsByMonthYearCompanyPaymentTypeStream = () ->  StreamSupport.stream(tripsByMonthYearCompanyPaymentTypes.spliterator(), false);


        c.output( OutputYearMonth.builder()
                .taxiTripByYearMonthCompanyKey(classCompositionByFlight.getTaxiTripByYearMonthCompanyKey())
                .totalTrips(classCompositionByFlight.getAmountOfTrips())
                .cashTrips(tripsByMonthYearCompanyPaymentTypeStream.get()
                        .filter(trip ->trip.getPaymentType().equals(PaymentType.CASH))
                        .map(trip -> trip.getTaxiTripByYearMonthCompanyCount().getAmountOfTrips())
                        .findFirst().orElse(0L))
                .noChargeTrips(tripsByMonthYearCompanyPaymentTypeStream.get()
                        .filter(trip ->trip.getPaymentType().equals(PaymentType.NO_CHARGE))
                        .map(trip -> trip.getTaxiTripByYearMonthCompanyCount().getAmountOfTrips())
                        .findFirst().orElse(0L))
                .creditCardTrips(tripsByMonthYearCompanyPaymentTypeStream.get()
                        .filter(trip ->trip.getPaymentType().equals(PaymentType.CREDIT_CARD))
                        .map(trip -> trip.getTaxiTripByYearMonthCompanyCount().getAmountOfTrips())
                        .findFirst().orElse(0L))
                .unkwonPaymentTrips(tripsByMonthYearCompanyPaymentTypeStream.get()
                        .filter(trip ->trip.getPaymentType().equals(PaymentType.UNKNOWN))
                        .map(trip -> trip.getTaxiTripByYearMonthCompanyCount().getAmountOfTrips())
                        .findFirst().orElse(0L))
                .build());
    }
}
