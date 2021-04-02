/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aip.processing.cli;

import com.google.common.net.HostAndPort;
import com.palantir.aip.processing.CliFrameOrchestrator;
import com.palantir.aip.processing.aip.AipInferenceProcessorClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

    private final AtomicLong frameId = new AtomicLong(0);

    public static AipInferenceProcessorClient grpc(HostAndPort hostAndPort, String productName, String productVersion) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
                .usePlaintext()
                .build();
        return new AipInferenceProcessorClient(channel, productName, productVersion);
    }

    public static void main(String... args) {
        System.exit(COMMAND_LINE.execute(args));
    }

    @Override
    public void run() {
        System.out.println("Orchestrator: running");
        System.out.println("Frames per second: " + framesPerSecond);

        System.out.println("Sending configuration request to server...");
        AipInferenceProcessorClient processor = grpc(
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

        CliFrameOrchestrator dispatcher = new CliFrameOrchestrator(sharedImagesDir, processor);
        long nanosPerFrame = (long) ((1.0 / framesPerSecond) * 1_000_000_000);
        ScheduledFuture<?> task = Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(
                        () -> dispatcher.send(frameId.getAndIncrement()), 0, nanosPerFrame, TimeUnit.NANOSECONDS);
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
}
