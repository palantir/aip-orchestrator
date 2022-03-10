package com.palantir.aip.processing.util;

import com.palantir.aip.processing.api.VideoFrame;
import com.palantir.aip.proto.processor.v2.ProcessorV2Protos;
import com.palantir.aip.proto.processor.v3.ProcessorV3Protos;
import com.palantir.aip.proto.types.PluginTypes;

public final class Converters {
    private Converters() {}

    public static PluginTypes.ImageFormat toV3(ProcessorV2Protos.ImageFormat format) {
        switch(format) {
            case RGB888:
                return PluginTypes.ImageFormat.RGB888;
            case PNG:
                return PluginTypes.ImageFormat.PNG;
            case TIFF:
                return PluginTypes.ImageFormat.TIFF;
            case BGR888:
                return PluginTypes.ImageFormat.BGR888;
            default:
                throw new IllegalArgumentException("invalid image format: " + format);
        }
    }

    public static ProcessorV2Protos.Image toV2(PluginTypes.Image image, PluginTypes.ImageFormat format) {
        switch (format) {
            case RGB888:
                return ProcessorV2Protos.Image.newBuilder()
                        .setRgbImage(ProcessorV2Protos.Rgb888Image.newBuilder()
                                .setWidth(image.getWidth())
                                .setHeight(image.getHeight())
                                .setPath(image.getPath()))
                        .build();
            case BGR888:
                return ProcessorV2Protos.Image.newBuilder()
                        .setBgrImage(ProcessorV2Protos.Bgr888Image.newBuilder()
                                .setWidth(image.getWidth())
                                .setHeight(image.getHeight())
                                .setPath(image.getPath()))
                        .build();
            case PNG:
                return ProcessorV2Protos.Image.newBuilder()
                        .setPngImage(ProcessorV2Protos.PngImage.newBuilder()
                                .setWidth(image.getWidth())
                                .setHeight(image.getHeight())
                                .setPath(image.getPath()))
                        .build();
            case TIFF:
                return ProcessorV2Protos.Image.newBuilder()
                        .setTiffImage(ProcessorV2Protos.TiffImage.newBuilder()
                                .setWidth(image.getWidth())
                                .setHeight(image.getHeight())
                                .setPath(image.getPath()))
                        .build();
            default:
                throw new RuntimeException(
                        "invalid image format: " + format);
        }
    }

    public static VideoFrame toVideoFrame(long frameId, ProcessorV3Protos.VideoRequest videoRequest, PluginTypes.ImageFormat imageFormat) {
        return new VideoFrame() {
            @Override
            public long streamId() {
                return 0;
            }

            @Override
            public long frameId() {
                return frameId;
            }

            @Override
            public ProcessorV2Protos.UasMetadata uasMetadata() {
                return toV2(videoRequest.getUas());
            }

            @Override
            public ProcessorV2Protos.Image image() {
                return toV2(videoRequest.getImage(), imageFormat);
            }
        };
    }

    public static ProcessorV2Protos.UasMetadata toV2(PluginTypes.UasMetadata uasMetadata) {
        return ProcessorV2Protos.UasMetadata.newBuilder()
                .setPlatformHeadingAngle(uasMetadata.getPlatformHeadingAngle())
                .setPlatformPitchAngle(uasMetadata.getPlatformPitchAngle())
                .setPlatformRollAngle(uasMetadata.getPlatformRollAngle())
                .setSensorLatitude(uasMetadata.getSensorLatitude())
                .setSensorLongitude(uasMetadata.getSensorLongitude())
                .setSensorTrueAltitude(uasMetadata.getSensorTrueAltitude())
                .setSensorHorizontalFov(uasMetadata.getSensorHorizontalFov())
                .setSensorVerticalFov(uasMetadata.getSensorVerticalFov())
                .setSensorRelativeAzimuthAngle(uasMetadata.getSensorRelativeAzimuthAngle())
                .setSensorRelativeElevationAngle(uasMetadata.getSensorRelativeElevationAngle())
                .setSensorRelativeRollAngle(uasMetadata.getSensorRelativeRollAngle())
                .build();
    }
}
