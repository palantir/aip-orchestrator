package com.palantir.aip.processing.orchestrators;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.aip.processing.aip.AipInferenceProcessorClientV3;
import com.palantir.aip.processing.util.ProcessorUtils;
import com.palantir.aip.proto.processor.v3.ProcessorV3Protos;
import com.palantir.aip.proto.types.PluginTypes;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class V3ProcessorOrchestrator {
    private final AipInferenceProcessorClientV3 processor;
    private final PluginTypes.Image testImage;
    private final PluginTypes.ImageFormat imageFormat;

    private long frameId;

    public V3ProcessorOrchestrator(Path sharedImagesDir, AipInferenceProcessorClientV3 processor) {
        this.frameId = 0;
        this.processor = processor;
        this.imageFormat = processor.getImageFormat();
        this.testImage = ProcessorUtils.loadAndSaveTestImage(this.imageFormat, sharedImagesDir);
    }

    public void sendVideoAtFixedRate(ScheduledExecutorService executor, long delay, TimeUnit timeUnit) {
        ScheduledFuture<?> task = executor.scheduleWithFixedDelay(this::sendVideo, 0, delay, timeUnit);

        try {
            System.out.println("Orchestrator: sending task...");
            // This future will not return unless the program has been interrupted
            task.get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Orchestrator: interrupted. Closing channel.");
            processor.closeChannel();
            throw new RuntimeException(e);
        }
    }

    public void sendImageryAtFixedRate(ScheduledExecutorService executor, long delay, TimeUnit timeUnit) {
        ScheduledFuture<?> task = executor.scheduleWithFixedDelay(this::sendImagery, 0, delay, timeUnit);

        try {
            System.out.println("Orchestrator: sending task...");
            // This future will not return unless the program has been interrupted
            task.get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Orchestrator: interrupted. Closing channel.");
            processor.closeChannel();
            throw new RuntimeException(e);
        }
    }

    private synchronized void sendVideo() {
        sendStartupIfFrameIdIsZero();

        ProcessorV3Protos.VideoRequest videoRequest =
                ProcessorUtils.buildTestVideoRequest(testImage, ProcessorUtils.constructSampleUasMetadata());
        ListenableFuture<ProcessorV3Protos.ProcessResponse> response =
                processor.process(ProcessorV3Protos.ProcessRequest.newBuilder()
                        .setRequestId(frameId++)
                        .setVideo(videoRequest)
                        .build());

        handleProcessResponse(response);
    }


    private synchronized void sendImagery() {
        sendStartupIfFrameIdIsZero();

        ProcessorV3Protos.ImageryRequest videoRequest =
                ProcessorUtils.buildTestImageryRequest(testImage);

        ListenableFuture<ProcessorV3Protos.ProcessResponse> response =
                processor.process(ProcessorV3Protos.ProcessRequest.newBuilder()
                        .setRequestId(frameId++)
                        .setImagery(videoRequest)
                        .build());

        handleProcessResponse(response);
    }

    private void handleProcessResponse(ListenableFuture<ProcessorV3Protos.ProcessResponse> result) {
        Futures.addCallback(
                result,
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(ProcessorV3Protos.ProcessResponse processResponse) {
                        System.out.println("Received ProcessResponse. RequestId: " + processResponse.getRequestId());
                        System.out.println("Process response object:");
                        System.out.println(processResponse);
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        System.out.println("Orchestrator request failed:");
                        throwable.printStackTrace();
                    }
                },
                MoreExecutors.directExecutor());
    }

    private void sendStartupIfFrameIdIsZero() {
        if (frameId == 0) {
            try {
                processor
                        .process(ProcessorV3Protos.ProcessRequest.newBuilder()
                                .setRequestId(frameId)
                                .setStartup(ProcessorV3Protos.StartupRequest.getDefaultInstance())
                                .build())
                        .get();
                frameId++;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
