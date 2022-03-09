/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aip.processing.orchestrators;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.aip.processing.aip.AipInferenceProcessorClientV2;
import com.palantir.aip.processing.api.VideoFrame;
import com.palantir.aip.processing.util.Converters;
import com.palantir.aip.processing.util.ProcessorUtils;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos.InferenceResponse;
import com.palantir.aip.proto.types.PluginTypes;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("BanSystemOut")
public final class V2ProcessorOrchestrator {
    private final AipInferenceProcessorClientV2 processor;
    private final PluginTypes.Image testImage;
    private final PluginTypes.ImageFormat imageFormat;

    private final AtomicLong frameId = new AtomicLong(0);

    public V2ProcessorOrchestrator(
            Path sharedImagesDir, AipInferenceProcessorClientV2 processor) {
        this.processor = processor;
        this.imageFormat = Converters.toV3(processor.getImageFormat());
        this.testImage = ProcessorUtils.loadAndSaveTestImage(this.imageFormat, sharedImagesDir);
    }

    public synchronized void send() {
        VideoFrame videoFrame = makePayload(frameId.getAndIncrement());
        System.out.println("Sending InferenceRequest. Stream id: " + videoFrame.streamId() +
                ", Frame id: " + videoFrame.frameId());
        ListenableFuture<InferenceResponse> result = processor.infer(videoFrame);

        Futures.addCallback(
                result,
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(InferenceResponse inferenceResponse) {
                        System.out.println("Received InferenceResponse. Stream id: " +
                                inferenceResponse.getIdentifier().getStreamId() + ", Frame id: " +
                                inferenceResponse.getIdentifier().getFrameId());
                        System.out.println("Inference response object:");
                        System.out.println(inferenceResponse);
                        processResponse(inferenceResponse);
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        System.out.println("Orchestrator request failed:");
                        System.out.println(throwable.getMessage());
                    }
                },
                MoreExecutors.directExecutor());
    }


    private void processResponse(InferenceResponse inferenceResponse) {
        long frameId = inferenceResponse.getIdentifier().getFrameId();
        System.out.println("Orchestrator received inference response for frame id " + frameId + ":");
        System.out.println(inferenceResponse.getInferences().getInferenceList());
        System.out.println("----------- End response for frame id " + frameId + " -----------");
    }

    private VideoFrame makePayload(long ref) {
        return Converters.toVideoFrame(
                ref,
                ProcessorUtils.buildTestVideoRequest(testImage, ProcessorUtils.constructSampleUasMetadata()),
                imageFormat);
    }
}
