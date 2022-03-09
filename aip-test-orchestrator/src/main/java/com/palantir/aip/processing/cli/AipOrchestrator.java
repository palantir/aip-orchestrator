/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aip.processing.cli;

import com.google.common.net.HostAndPort;
import com.palantir.aip.processing.aip.AipInferenceProcessorClientV3;
import com.palantir.aip.processing.orchestrators.V2ProcessorOrchestrator;
import com.palantir.aip.processing.aip.AipInferenceProcessorClientV2;
import com.palantir.aip.processing.orchestrators.V3ProcessorOrchestrator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.*;

import picocli.CommandLine;

@CommandLine.Command(
        name = "orchestrator",
        mixinStandardHelpOptions = true,
        description = "Runs a test orchestrator that sends frames to processors.")
@SuppressWarnings("BanSystemOut")
public final class AipOrchestrator implements Runnable {
    private static final CommandLine.Help.ColorScheme NO_COLORS =
            new CommandLine.Help.ColorScheme.Builder(CommandLine.Help.Ansi.OFF).build();
    private static final CommandLine COMMAND_LINE =
            new CommandLine(new AipOrchestrator()).setUsageHelpAutoWidth(true).setColorScheme(NO_COLORS);

    @CommandLine.Option(
            names = "--shared-images-dir",
            description = "The directory path that frames should be written to and shared with the processor.",
            defaultValue = "/tmp")
    private Path sharedImagesDir;

    @CommandLine.Option(
            names = "--uri",
            description = "The URI of the inference processor to connect to.",
            defaultValue = "grpc://localhost:50051")
    private URI uri;

    @CommandLine.Option(
            names = "--rate",
            description = "The number of frames per second to send to the processor (can be a decimal).",
            defaultValue = "0.2")
    private double framesPerSecond;

    @CommandLine.Option(
            names = "--version",
            description = "The version of processor to test. Valid options are: v2, v3video, v3imagery",
            defaultValue = "v3video"
    )
    private String version;



    public static AipInferenceProcessorClientV2 grpcV2(HostAndPort hostAndPort, String productName, String productVersion) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
                .usePlaintext()
                .build();
        return new AipInferenceProcessorClientV2(channel, productName, productVersion);
    }

    public static AipInferenceProcessorClientV3 grpcV3(HostAndPort hostAndPort, String productName, String productVersion) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
                .usePlaintext()
                .build();
        return new AipInferenceProcessorClientV3(channel, productName, productVersion);
    }

    public static void main(String... args) {
        System.exit(COMMAND_LINE.execute(args));
    }

    @Override
    public void run() {
        System.out.println("Orchestrator: running");
        System.out.println("Frames per second: " + framesPerSecond);

        switch(version.toLowerCase()) {
            case "v2":
                handleV2();
                break;
            case "v3video":
                handleV3Video();
                break;
            case "v3imagery":
                handleV3Imagery();
                break;
        }
    }

    private void handleV2() {
        System.out.println("Sending configuration request to server...");
        AipInferenceProcessorClientV2 processor = grpcV2(
                        HostAndPort.fromParts(uri.getHost(), uri.getPort()),
                        "AIP Orchestrator",
                        Optional.ofNullable(AipOrchestrator.class.getPackage().getImplementationVersion())
                                .orElse("0.0.0"));

        try {
            processor.configure();
        } catch (RuntimeException e) {
            System.out.println("Error when initializing processor" + e.toString());
            throw e;
        }
        System.out.println("Processor configured. Getting ready to send inference requests.");

        V2ProcessorOrchestrator dispatcher = new V2ProcessorOrchestrator(sharedImagesDir, processor);

        long nanosPerFrame = (long) ((1.0 / framesPerSecond) * 1_000_000_000);
        ScheduledExecutorService executor =  Executors.newScheduledThreadPool(1);
        dispatcher.sendAtFixedRate(executor, nanosPerFrame, TimeUnit.NANOSECONDS);
    }

    private void handleV3Video() {
        System.out.println("Sending configuration request to server...");
        AipInferenceProcessorClientV3 processor = grpcV3(
                HostAndPort.fromParts(uri.getHost(), uri.getPort()),
                "AIP Orchestrator",
                Optional.ofNullable(AipOrchestrator.class.getPackage().getImplementationVersion())
                        .orElse("0.0.0"));

        try {
            processor.configure();
        } catch (RuntimeException e) {
            System.out.println("Error when initializing processor" + e.toString());
            throw e;
        }
        System.out.println("Processor configured. Getting ready to send inference requests.");

        V3ProcessorOrchestrator dispatcher = new V3ProcessorOrchestrator(sharedImagesDir, processor);

        long nanosPerFrame = (long) ((1.0 / framesPerSecond) * 1_000_000_000);
        ScheduledExecutorService executor =  Executors.newScheduledThreadPool(1);
        dispatcher.sendVideoAtFixedRate(executor, nanosPerFrame, TimeUnit.NANOSECONDS);
    }

    private void handleV3Imagery() {
        System.out.println("Sending configuration request to server...");
        AipInferenceProcessorClientV3 processor = grpcV3(
                HostAndPort.fromParts(uri.getHost(), uri.getPort()),
                "AIP Orchestrator",
                Optional.ofNullable(AipOrchestrator.class.getPackage().getImplementationVersion())
                        .orElse("0.0.0"));

        try {
            processor.configure();
        } catch (RuntimeException e) {
            System.out.println("Error when initializing processor" + e);
            throw e;
        }
        System.out.println("Processor configured. Getting ready to send inference requests.");

        V3ProcessorOrchestrator dispatcher = new V3ProcessorOrchestrator(sharedImagesDir, processor);

        long nanosPerFrame = (long) ((1.0 / framesPerSecond) * 1_000_000_000);
        ScheduledExecutorService executor =  Executors.newScheduledThreadPool(1);
        dispatcher.sendImageryAtFixedRate(executor, nanosPerFrame, TimeUnit.NANOSECONDS);
    }
}
