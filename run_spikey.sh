#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS=$PWD/spikey-dataflow-236900-0f1295a75566.json
export BUCKET=spikey-data-flow-store
export PROJECT=spikey-dataflow-236900

go run main.go \
    --runner dataflow \
    --project ${PROJECT?} \
    --temp_location gs://${BUCKET?}/tmp/ \
    --staging_location gs://${BUCKET?}/binaries/ \
    --worker_harness_container_image=zerbitx/beam-go:latest