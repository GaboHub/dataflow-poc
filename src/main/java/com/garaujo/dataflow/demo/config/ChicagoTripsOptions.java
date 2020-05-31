package com.garaujo.dataflow.demo.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface ChicagoTripsOptions extends PipelineOptions {


    /** Set this required option to specify where to write the output. */
    //@Validation.Required
    @Default.String("gs://staging-df-test-2/chicago_trips-2/chicago_taxi_trips")
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> output);

    /** Set this required option to specify where to write the output. */
    @Validation.Required
    @Default.Integer(1000)
    ValueProvider<Integer> getRecordLimit();

    void setRecordLimit(ValueProvider<Integer> recordLimit);
}
