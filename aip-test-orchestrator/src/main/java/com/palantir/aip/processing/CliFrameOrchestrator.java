/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import java.awt.*;
import java.awt.image.*;
import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import javax.imageio.ImageIO;

@SuppressWarnings("BanSystemOut")
public final class CliFrameOrchestrator {
    private final AipInferenceProcessorClient processor;
    private final ProcessorV2Protos.Image testImage;
    private static final int height = 2048;
    private static final int width = 2048;
    private static final String testImageResourcePath = "images/testImage.bgr888";

    public CliFrameOrchestrator(
            Path sharedImagesDir, AipInferenceProcessorClient processor) {
        this.processor = processor;
        this.testImage = loadAndSaveTestImage(processor.getImageFormat(), sharedImagesDir);
    }

    public synchronized void send(Long ref) {
        VideoFrame videoFrame = makePayload(ref);
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

    public static File createTempImageFileForProcessor(Path imagesDirPath, byte[] bytes, ProcessorV2Protos.ImageFormat imageFormat)
            throws IOException {
        String imageEncoding;
        switch(imageFormat) {
            case PNG:
                imageEncoding = ".png";
                break;
            case TIFF:
                imageEncoding = ".tiff";
                break;
            case RGB888:
                imageEncoding = ".rgb888";
                break;
            case BGR888:
                imageEncoding = ".bgr888";
                break;
            default:
                throw new RuntimeException(
                        "invalid image format specified by processor: " + imageFormat);
        }
        File tempFile = File.createTempFile("testImage", imageEncoding, new File(imagesDirPath.toString()));
        tempFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(tempFile);
        outputStream.write(bytes);
        return tempFile;
    }

    private ProcessorV2Protos.Image constructImage(File tempFile) {
        switch (processor.getImageFormat()) {
            case RGB888:
                return ProcessorV2Protos.Image.newBuilder()
                        .setRgbImage(ProcessorV2Protos.Rgb888Image.newBuilder()
                                .setWidth(width)
                                .setHeight(height)
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            case BGR888:
                return ProcessorV2Protos.Image.newBuilder()
                        .setBgrImage(ProcessorV2Protos.Bgr888Image.newBuilder()
                                .setWidth(width)
                                .setHeight(height)
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            case PNG:
                return ProcessorV2Protos.Image.newBuilder()
                        .setPngImage(ProcessorV2Protos.PngImage.newBuilder()
                                .setWidth(width)
                                .setHeight(height)
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            case TIFF:
                return ProcessorV2Protos.Image.newBuilder()
                        .setTiffImage(ProcessorV2Protos.TiffImage.newBuilder()
                                .setWidth(width)
                                .setHeight(height)
                                .setPath(tempFile.getAbsolutePath()))
                        .build();
            default:
                throw new RuntimeException(
                        "invalid image format specified by processor: " + processor.getImageFormat());
        }
    }

    private ProcessorV2Protos.Image loadAndSaveTestImage(ProcessorV2Protos.ImageFormat imageFormat, Path sharedImagesDir) {
        URL imageResourceBgr = CliFrameOrchestrator.class.getClassLoader().getResource(testImageResourcePath);
        try {
            byte[] bytes = imageResourceBgr.openStream().readAllBytes();

            switch (imageFormat) {
                case BGR888:
                    break;
                case RGB888:
                    for (int i = 0; i < bytes.length; i += 3) {
                        // Swap 1st and 3rd component
                        byte b = bytes[i];
                        bytes[i] = bytes[i + 2];
                        bytes[i + 2] = b;
                    }
                    break;
                case PNG:
                case TIFF:
                    BufferedImage bufferedImage =
                            new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
                    bufferedImage.setData(Raster.createRaster(
                            bufferedImage.getSampleModel(), new DataBufferByte(bytes, bytes.length), new Point()));

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    if (imageFormat == ProcessorV2Protos.ImageFormat.PNG) {
                        ImageIO.write(bufferedImage, "png", baos);
                    } else if(imageFormat == ProcessorV2Protos.ImageFormat.TIFF) {
                        ImageIO.write(bufferedImage, "tiff", baos);
                    }
                    bytes = baos.toByteArray();
                    break;
                default:
                    throw new RuntimeException(
                            "invalid image format specified by processor: " + imageFormat);
            }

            File tempFile = createTempImageFileForProcessor(sharedImagesDir, bytes, imageFormat);
            System.out.println("Created test image at location:" + tempFile.getPath());
            return constructImage(tempFile);
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
