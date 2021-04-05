/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aip.processing.aip;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.aip.processing.GrpcCalls;
import com.palantir.aip.processing.api.VideoFrame;
import com.palantir.aip.proto.configuration.ConfigProtos;
import com.palantir.aip.proto.configuration.ConfigurationServiceGrpc;
import com.palantir.aip.proto.configuration.ConfigurationServiceGrpc.ConfigurationServiceBlockingStub;
import com.palantir.aip.proto.processor.v2.ProcessingServiceGrpc;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos.ImageFormat;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos.InferenceResponse;
import io.grpc.ManagedChannel;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class AipInferenceProcessorClient {
    private final ManagedChannel channel;
    private final ConfigProtos.ConfigurationRequest configRequest;

    private ProcessingServiceGrpc.ProcessingServiceStub stub;
    private ImageFormat imageFormat;

    private static final double NANOS_PER_TICK = TimeUnit.SECONDS.toNanos(1) / 90000.0;

    public AipInferenceProcessorClient(ManagedChannel channel, String productName, String productVersion) {
        this.channel = channel;
        this.configRequest = ConfigProtos.ConfigurationRequest.newBuilder()
                .setOrchestratorName(productName)
                .setOrchestratorVersion(productVersion)
                .build();
        this.stub = ProcessingServiceGrpc.newStub(channel);
    }

    public ImageFormat getImageFormat() {
        return imageFormat;
    }

    public void configure() {
        ConfigurationServiceBlockingStub blockingStub = ConfigurationServiceGrpc.newBlockingStub(channel);
        System.out.println("Sending config request..");
        System.out.println(configRequest);
        ConfigProtos.ConfigurationResponse configResponse = blockingStub.configure(configRequest);
        System.out.println("Received config response.");
        System.out.println(configResponse);
        handleConfigurationResponse(configResponse);
    }

    public Optional<ListenableFuture<InferenceResponse>> infer(VideoFrame videoFrame) {
        ProcessorV2Protos.InferenceRequest request = ProcessorV2Protos.InferenceRequest.newBuilder()
                .setHeader(getHeader(videoFrame))
                .setFrame(getFrameMessage(videoFrame))
                .build();
        return Optional.of(GrpcCalls.call(this.stub::infer, request));
    }

    private void handleConfigurationResponse(ConfigProtos.ConfigurationResponse response) {
        ImageFormat imageFormatResponse = response.getVersion().getV2().getImageFormat();
        switch (imageFormatResponse) {
            case RGB888:
            case BGR888:
            case PNG:
            case TIFF:
                this.imageFormat = imageFormatResponse;
                break;
            default:
                throw new RuntimeException(
                        "invalid image format specified by processor: " + imageFormatResponse);
        }
    }

    private ProcessorV2Protos.RequestHeader getHeader(VideoFrame videoFrame) {
        return ProcessorV2Protos.RequestHeader.newBuilder()
                .setIdentifier(ProcessorV2Protos.Identifier.newBuilder()
                        .setStreamId(videoFrame.streamId())
                        .setFrameId(videoFrame.frameId())
                        .build())
                .setDeadline(
                        com.google.protobuf.Duration.newBuilder().setSeconds(30).build())
                .setTimestamp(ProcessorV2Protos.Timestamp.newBuilder()
                        .setNanos((long) (videoFrame.frameId() * NANOS_PER_TICK))
                        .build())
                .build();
    }

    private ProcessorV2Protos.Frame getFrameMessage(VideoFrame frame) {
        return ProcessorV2Protos.Frame.newBuilder()
                .setImage(frame.image())
                .setUasMetadata(frame.uasMetadata())
                .build();
    }

    public void closeChannel() {
        channel.shutdown();
    }
}
