package dofntest;

import com.garaujo.dataflow.demo.models.TaxiTripBQRow;
import com.garaujo.dataflow.demo.models.enums.PaymentType;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StringRowToTaxiTripFnTest {

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void shouldReturnTaxiTripInstanceGivenStringRow() {

        String row = "704ed277af328aaf32cf71871a58fa2584d48de3,a800ab3336297d1414f214106679e5d59cdb05a084de974d10efbf64008b94f933a8c1dd0e5f6de2f4c0ed27d51fe0f0dad512717369832ab467fa65c3716e58,2017-06-14 18:00:00 UTC,2017-06-14 19:00:00 UTC,3240,0.0,,,14,,50.5,0.0,0.0,0.0,50.5,Cash,Northwest Management LLC,41.968069,-87.721559063,POINT (-87.7215590627 41.968069),,,";

        final TaxiTripBQRow expected = TaxiTripBQRow.builder()
                    .uniqueKey("704ed277af328aaf32cf71871a58fa2584d48de3")
                    .taxiId("a800ab3336297d1414f214106679e5d59cdb05a084de974d10efbf64008b94f933a8c1dd0e5f6de2f4c0ed27d51fe0f0dad512717369832ab467fa65c3716e58")
                    .tripStart(LocalDateTime.of(2017, 6, 14,18,0,0))
                    .tripEnd(LocalDateTime.of(2017, 6, 14,19,0,0))
                    .tripSecondsDuration(3240L)
                    .tripMiles(0.0)
                    .pickupCensusTract(null)
                    .dropOffCensusTract(null)
                    .pickUpCommunityArea(14)
                    .dropOffCommunityArea(null)
                    .fare(50.5)
                    .tips(0.0)
                    .tolls(0.0)
                    .extras(0.0)
                    .tripTotal(50.5)
                    .paymentType(PaymentType.CASH)
                    .company("Northwest Management LLC")
                    .pickUpLatitude(41.968069)
                    .pickUpLongitude(-87.721559063)
                    .pickUpLocation("POINT (-87.7215590627 41.968069)")
                    .dropOffLatitude(null)
                    .dropOffLongitude(null)
                    .dropOffLocation("")

                .build();

        PCollection<TaxiTripBQRow> output = p.apply(Create.of(row).withCoder(StringUtf8Coder.of()))
                .apply(ParDo.of(new StringRowToTaxiTripFn()));

        PAssert.that(output).satisfies(rows -> {
            assertEquals(expected, rows.iterator().next());
            return null;
        });
        p.run().waitUntilFinish();


    }

    @Test
    public void shouldReturnTaxiTripNoChargeInstanceGivenStringRow() {

        String row = "704ed277af328aaf32cf71871a58fa2584d48de3,a800ab3336297d1414f214106679e5d59cdb05a084de974d10efbf64008b94f933a8c1dd0e5f6de2f4c0ed27d51fe0f0dad512717369832ab467fa65c3716e58,2017-06-14 18:00:00 UTC,2017-06-14 19:00:00 UTC,3240,0.0,,,14,,50.5,0.0,0.0,0.0,50.5,No Charge,Northwest Management LLC,41.968069,-87.721559063,POINT (-87.7215590627 41.968069),,,";

        TaxiTripBQRow expected = TaxiTripBQRow.builder()
                .uniqueKey("704ed277af328aaf32cf71871a58fa2584d48de3")
                .taxiId("a800ab3336297d1414f214106679e5d59cdb05a084de974d10efbf64008b94f933a8c1dd0e5f6de2f4c0ed27d51fe0f0dad512717369832ab467fa65c3716e58")
                .tripStart(LocalDateTime.of(2017, 6, 14,18,0,0))
                .tripEnd(LocalDateTime.of(2017, 6, 14,19,0,0))
                .tripSecondsDuration(3240L)
                .tripMiles(0.0)
                .pickupCensusTract(null)
                .dropOffCensusTract(null)
                .pickUpCommunityArea(14)
                .dropOffCommunityArea(null)
                .fare(50.5)
                .tips(0.0)
                .tolls(0.0)
                .extras(0.0)
                .tripTotal(50.5)
                .paymentType(PaymentType.NO_CHARGE)
                .company("Northwest Management LLC")
                .pickUpLatitude(41.968069)
                .pickUpLongitude(-87.721559063)
                .pickUpLocation("POINT (-87.7215590627 41.968069)")
                .dropOffLatitude(null)
                .dropOffLongitude(null)
                .dropOffLocation("")

                .build();

        PCollection<TaxiTripBQRow> output = p.apply(Create.of(row).withCoder(StringUtf8Coder.of()))
                .apply(ParDo.of(new StringRowToTaxiTripFn()));


        PAssert.that(output).satisfies(rows -> {
            assertEquals(expected, rows.iterator().next());
            return null;
        });
        p.run().waitUntilFinish();

    }

    @Test
    public void shouldReturnTaxiTripWithoutEmptylinesOnTheGivenStringRow() {
        String row = "704ed277af328aaf32cf71871a58fa2584d48de3,a800ab3336297d1414f214106679e5d59cdb05a084de974d10efbf64008b94f933a8c1dd0e5f6de2f4c0ed27d51fe0f0dad512717369832ab467fa65c3716e58,2017-06-14 18:00:00 UTC,2017-06-14 19:00:00 UTC,3240,0.0,0,0,14,0,50.5,0.0,0.0,0.0,50.5,No Charge,Northwest Management LLC,41.968069,-87.721559063,POINT (-87.7215590627 41.968069),0,0,0";

        TaxiTripBQRow expected = TaxiTripBQRow.builder()
                .uniqueKey("704ed277af328aaf32cf71871a58fa2584d48de3")
                .taxiId("a800ab3336297d1414f214106679e5d59cdb05a084de974d10efbf64008b94f933a8c1dd0e5f6de2f4c0ed27d51fe0f0dad512717369832ab467fa65c3716e58")
                .tripStart(LocalDateTime.of(2017, 6, 14,18,0,0))
                .tripEnd(LocalDateTime.of(2017, 6, 14,19,0,0))
                .tripSecondsDuration(3240L)
                .tripMiles(0.0)
                .pickupCensusTract(0L)
                .dropOffCensusTract(0L)
                .pickUpCommunityArea(14)
                .dropOffCommunityArea(0)
                .fare(50.5)
                .tips(0.0)
                .tolls(0.0)
                .extras(0.0)
                .tripTotal(50.5)
                .paymentType(PaymentType.NO_CHARGE)
                .company("Northwest Management LLC")
                .pickUpLatitude(41.968069)
                .pickUpLongitude(-87.721559063)
                .pickUpLocation("POINT (-87.7215590627 41.968069)")
                .dropOffLatitude(0.0)
                .dropOffLongitude(0.0)
                .dropOffLocation("0")

                .build();

        PCollection<TaxiTripBQRow> output = p.apply(Create.of(row).withCoder(StringUtf8Coder.of()))
                .apply(ParDo.of(new StringRowToTaxiTripFn()));


        PAssert.that(output).satisfies(rows -> {
            assertEquals(expected, rows.iterator().next());
            return null;
        });
        p.run().waitUntilFinish();
    }
}
