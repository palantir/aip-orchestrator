package com.palantir.aip.processing.util;

import com.palantir.aip.processing.orchestrators.V2ProcessorOrchestrator;
import com.palantir.aip.proto.processor.v3.ProcessorV3Protos;
import com.palantir.aip.proto.types.PluginTypes;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.Point;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.time.Instant;

public final class ProcessorUtils {
    private static final String TEST_IMAGE_RESOURCE_PATH = "images/testImage.bgr888";
    private static final int HEIGHT = 2048;
    private static final int WIDTH = 2048;

    private ProcessorUtils() {}

    public static PluginTypes.Image loadAndSaveTestImage(PluginTypes.ImageFormat imageFormat, Path sharedImagesDir) {
        URL imageResourceBgr = V2ProcessorOrchestrator.class.getClassLoader().getResource(TEST_IMAGE_RESOURCE_PATH);
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
                    BufferedImage bufferedImage = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_3BYTE_BGR);
                    bufferedImage.setData(Raster.createRaster(
                            bufferedImage.getSampleModel(), new DataBufferByte(bytes, bytes.length), new Point()));

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    if (imageFormat == PluginTypes.ImageFormat.PNG) {
                        ImageIO.write(bufferedImage, "png", baos);
                    } else if (imageFormat == PluginTypes.ImageFormat.TIFF) {
                        ImageIO.write(bufferedImage, "tiff", baos);
                    }
                    bytes = baos.toByteArray();
                    break;
                default:
                    throw new RuntimeException("invalid image format specified by processor: " + imageFormat);
            }

            File tempFile = createTempImageFileForProcessor(sharedImagesDir, bytes, imageFormat);
            System.out.println("Created test image at location:" + tempFile.getPath());
            return PluginTypes.Image.newBuilder()
                    .setPath(tempFile.getAbsolutePath())
                    .setWidth(WIDTH)
                    .setHeight(HEIGHT)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File createTempImageFileForProcessor(
            Path imagesDirPath, byte[] bytes, PluginTypes.ImageFormat imageFormat) throws IOException {
        String imageEncoding;
        switch (imageFormat) {
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
                throw new RuntimeException("invalid image format specified by processor: " + imageFormat);
        }
        File tempFile = File.createTempFile("testImage", imageEncoding, new File(imagesDirPath.toString()));
        tempFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(tempFile);
        outputStream.write(bytes);
        return tempFile;
    }

    public static ProcessorV3Protos.VideoRequest buildTestVideoRequest(
            PluginTypes.Image image, PluginTypes.UasMetadata uasMetadata) {
        return ProcessorV3Protos.VideoRequest.newBuilder()
                .setImage(image)
                .setHeight(image.getHeight())
                .setWidth(image.getWidth())
                .setUas(uasMetadata)
                .setPts(Instant.now().toEpochMilli())
                .build();
    }

    public static PluginTypes.UasMetadata constructSampleUasMetadata() {
        return PluginTypes.UasMetadata.newBuilder()
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
}
