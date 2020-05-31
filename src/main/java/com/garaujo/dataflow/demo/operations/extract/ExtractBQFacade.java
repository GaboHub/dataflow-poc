package com.garaujo.dataflow.demo.operations.extract;

import com.garaujo.dataflow.demo.models.TaxiTripBQRow;
import com.garaujo.dataflow.demo.models.enums.PaymentType;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ExtractBQFacade {

    private ExtractBQFacade() {
        throw new IllegalArgumentException();
    }

    public static PCollection<TaxiTripBQRow> getTaxiTripRow(Pipeline p) {

        List<String> fields = new ArrayList<>();
        fields.add("company");
        fields.add("payment_type");
        fields.add("trip_start_timestamp");
        fields.add("trip_end_timestamp");

        ValueProvider<List<String>> selectedFields = ValueProvider.StaticValueProvider.of(fields);

        return p.apply("Read Taxi Trip rows", BigQueryIO.read((SerializableFunction<SchemaAndRecord, TaxiTripBQRow>) schemaAndRecord -> {
            GenericRecord record = schemaAndRecord.getRecord();
            return TaxiTripBQRow.builder()
                    .tripStart(getDate(record, "trip_start_timestamp"))
                    .tripEnd(getDate(record, "trip_end_timestamp"))
                    .paymentType(PaymentType.getPaymentType(String.valueOf(record.get("payment_type"))))
                    .company(String.valueOf(record.get("company")))
                    .build();
        }).from("bigquery-public-data:chicago_taxi_trips.taxi_trips")
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                .withSelectedFields(selectedFields)
                .withTemplateCompatibility()
                .withCoder(SerializableCoder.of(TaxiTripBQRow.class))
                .withoutValidation());
    }

    /*
    public static PCollection<BidPriceBQ> getBidPriceBQ(Pipeline p, SolutionOption options) {
        return p.apply("Read solution bid price headers", BigQueryIO.read((SerializableFunction<SchemaAndRecord, BidPriceBQ>) schemaAndRecord -> {
            GenericRecord record = schemaAndRecord.getRecord();
            return BidPriceBQ.builder()
                    .solutionBQ(SolutionBQ.builder()
                            .carrier(String.valueOf(record.get("BDPLDR_CARRIER_CD")))
                            .flightNumber(Integer.parseInt(String.valueOf(record.get("BDPLDR_FLIGHT_NUM"))))
                            .flightDate(String.valueOf(record.get("BDPLDR_FLIGHT_DT")))
                            .cabin(String.valueOf(record.get("BDPLDR_CABIN_CD")))
                            .origin(String.valueOf(record.get("BDPLDR_ORIGIN_CD")))
                            .destination(String.valueOf(record.get("BDPLDR_DESTINATION_CD")))
                            .departureDate(String.valueOf(record.get("BDPLDR_DEPARTURE_DT")))
                            .build())
                    .seatIndex(Integer.parseInt(String.valueOf(record.get("BDPLDR_SEAT_NUM"))))
                    .seatValue(Integer.parseInt(String.valueOf(record.get("BDPLDR_SEAT_VAL"))))
                    .build();
        }).from(options.getBidPriceTableName())
                .withCoder(SerializableCoder.of(BidPriceBQ.class))
                .withoutValidation());
    */

    private static Long getLongValueFromLine(String line) {
        if (line == null || line.trim().isEmpty() || "null".equals(line)) {
            return null;
        }

        return Long.parseLong(line);
    }

    private static Double getDoubleValueFromLine(String line) {
        if (line == null || line.trim().isEmpty() || "null".equals(line)) {
            return null;
        }

        return Double.parseDouble(line);
    }

    private static Integer getIntegerValueFromLine(String line) {
        if (line == null || line.trim().isEmpty() || "null".equals(line)) {
            return null;
        }

        return Integer.parseInt(line);
    }

    private static LocalDateTime getDate(GenericRecord record, String column) {

        String line = String.valueOf(record.get(column));
        if (line == null || line.trim().isEmpty() || "null".equals(line)) {
            return null;
        }
        long dateEpoch = Long.parseLong(line) / 1000000;
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(dateEpoch), ZoneId.systemDefault());
    }

}
