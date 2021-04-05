/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aip.processing;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.aip.processing.aip.AipInferenceProcessorClient;
import com.palantir.aip.processing.api.VideoFrame;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos.InferenceResponse;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos.UasMetadata;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Optional;
import javax.imageio.ImageIO;

@SuppressWarnings("BanSystemOut")
public final class CliFrameOrchestrator {
    private final AipInferenceProcessorClient processor;
    private final ProcessorV2Protos.Image testImage;

    public CliFrameOrchestrator(
            Path sharedImagesDir, String testImageResourceName, AipInferenceProcessorClient processor) {
        this.processor = processor;
        this.testImage = loadAndSaveTestImage("images/" + testImageResourceName, sharedImagesDir);
    }

    public synchronized void send(Long ref) {
        VideoFrame videoFrame = makePayload(ref);
        System.out.println("Sending InferenceRequest. Stream id: " + videoFrame.streamId() +
                ", Frame id: " + videoFrame.frameId());
        Optional<ListenableFuture<InferenceResponse>> result = processor.infer(videoFrame);
        if (result.isEmpty()) {
            throw new RuntimeException("session is closed");
        }

        Futures.addCallback(
                result.get(),
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

    public static File createTempImageFileForProcessor(BufferedImage bufferedImage, Path imagesDirPath)
            throws IOException {
        final File tempFile = File.createTempFile("testImage", ".png", new File(imagesDirPath.toString()));
        tempFile.deleteOnExit();
        ImageIO.write(bufferedImage, "png", tempFile);
        return tempFile;
    }

    private ProcessorV2Protos.Image constructImage(BufferedImage bufferedImage, File tempFile) {
        switch (processor.getImageFormat()) {
            case RGB888:
                return ProcessorV2Protos.Image.newBuilder()
                        .setRgbImage(ProcessorV2Protos.Rgb888Image.newBuilder()
                                .setWidth(bufferedImage.getWidth())
                                .setHeight(bufferedImage.getHeight())
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            case BGR888:
                return ProcessorV2Protos.Image.newBuilder()
                        .setBgrImage(ProcessorV2Protos.Bgr888Image.newBuilder()
                                .setWidth(bufferedImage.getWidth())
                                .setHeight(bufferedImage.getHeight())
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            case PNG:
                return ProcessorV2Protos.Image.newBuilder()
                        .setPngImage(ProcessorV2Protos.PngImage.newBuilder()
                                .setWidth(bufferedImage.getWidth())
                                .setHeight(bufferedImage.getHeight())
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            case TIFF:
                return ProcessorV2Protos.Image.newBuilder()
                        .setTiffImage(ProcessorV2Protos.TiffImage.newBuilder()
                                .setWidth(bufferedImage.getWidth())
                                .setHeight(bufferedImage.getHeight())
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            default:
                throw new RuntimeException(
                        "invalid image format specified by processor: " + processor.getImageFormat());
        }
    }

    private ProcessorV2Protos.Image loadAndSaveTestImage(String testImageResourcePath, Path sharedImagesDir) {
        URL imageResource = CliFrameOrchestrator.class.getClassLoader().getResource(testImageResourcePath);
        try {
            BufferedImage bufferedImage = ImageIO.read(imageResource);
            File tempFile = createTempImageFileForProcessor(bufferedImage, sharedImagesDir);
            System.out.println("Created test image at location:" + tempFile.getPath());
            return constructImage(bufferedImage, tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processResponse(InferenceResponse inferenceResponse) {
        long frameId = inferenceResponse.getIdentifier().getFrameId();
        System.out.println("Orchestrator received inference response for frame id " + frameId + ":");
        System.out.println(inferenceResponse.getInferences().getInferenceList());
        System.out.println("----------- End response for frame id " + frameId + " -----------");
    }

    private UasMetadata constructSampleUasMetadata() {
        return ProcessorV2Protos.UasMetadata.newBuilder()
                .setPlatformHeadingAngle((float) 196.22980086976426)
                .setPlatformPitchAngle((float) 2.0374156926175724)
                .setPlatformRollAngle((float) 6.805627613147374)
                .setSensorLatitude(33.0033516624026)
                .setSensorLongitude(-110.78985552834187)
                .setSensorTrueAltitude(0.0)
                .setSensorHorizontalFov((float) 0.409246967269398)
                .setSensorVerticalFov((float) 0.22796978713664454)
                .setSensorRelativeAzimuthAngle(113.56913759223399)
                .setSensorRelativeElevationAngle(0.0)
                .setSensorRelativeRollAngle(0.0)
                .build();
    }

    private VideoFrame makePayload(long ref) {
        return new VideoFrame() {
            @Override
            public long streamId() {
                return 0;
            }

            @Override
            public long frameId() {
                return ref;
            }

            @Override
            public ProcessorV2Protos.Image image() {
                return testImage;
            }

            @Override
            public UasMetadata uasMetadata() {
                return constructSampleUasMetadata();
            }
        };
    }
}
