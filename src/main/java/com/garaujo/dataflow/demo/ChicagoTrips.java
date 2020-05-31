package com.garaujo.dataflow.demo;

import com.garaujo.dataflow.demo.config.ChicagoTripsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class ChicagoTrips {

    public static void main(String[] args) {
        ChicagoTripsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ChicagoTripsOptions.class);
        ChicagoTripsFacade chicagoTripsFacade = new ChicagoTripsFacade();
        chicagoTripsFacade.run(options);
    }
}
