/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aip.processing.api;

import com.palantir.aip.processing.aip.AipInferenceProcessorClient;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos;

/**
 * Information about a single frame of a video stream.
 *
 * A frame is transient, and should not be held on to after the call to
 * {@link AipInferenceProcessorClient#infer} returns. In
 * particular, the returned future should not have access to the Frame.
 */
public interface VideoFrame {
    long streamId();

    long frameId();

    ProcessorV2Protos.UasMetadata uasMetadata();

    ProcessorV2Protos.Image image();
}
