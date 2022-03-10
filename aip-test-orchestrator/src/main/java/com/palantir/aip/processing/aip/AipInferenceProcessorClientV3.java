package com.palantir.aip.processing.aip;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.aip.proto.configuration.ConfigProtos;
import com.palantir.aip.proto.configuration.ConfigurationServiceGrpc;
import com.palantir.aip.proto.processor.v3.ProcessingServiceGrpc;
import com.palantir.aip.proto.processor.v3.ProcessorV3Protos.ProcessRequest;
import com.palantir.aip.proto.processor.v3.ProcessorV3Protos.ProcessResponse;
import com.palantir.aip.proto.types.PluginTypes;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class AipInferenceProcessorClientV3 {
    private final ManagedChannel channel;
    private final ConfigProtos.ConfigurationRequest configRequest;

    private ProcessingServiceGrpc.ProcessingServiceStub stub;
    private PluginTypes.ImageFormat imageFormat;
    private Map<Long, SettableFuture<ProcessResponse>> futures = new ConcurrentHashMap<>();

    private StreamObserver<ProcessRequest> requestObserver;

    private boolean closed = false;
    private boolean supportsRaw = false;

    private static final double NANOS_PER_TICK = TimeUnit.SECONDS.toNanos(1) / 90000.0;

    public AipInferenceProcessorClientV3(ManagedChannel channel, String productName, String productVersion) {
        this.channel = channel;
        this.configRequest = ConfigProtos.ConfigurationRequest.newBuilder()
                .setOrchestratorName(productName)
                .setOrchestratorVersion(productVersion)
                .build();
        this.stub = ProcessingServiceGrpc.newStub(channel);
    }

    public PluginTypes.ImageFormat getImageFormat() {
        return imageFormat;
    }

    public boolean getSupportsRawImagery() {
        return supportsRaw;
    }

    public void configure() {
        ConfigurationServiceGrpc.ConfigurationServiceBlockingStub blockingStub = ConfigurationServiceGrpc.newBlockingStub(channel);
        System.out.println("Sending config request..");
        System.out.println(configRequest);
        ConfigProtos.ConfigurationResponse configResponse = blockingStub.configure(configRequest);
        System.out.println("Received config response.");
        System.out.println(configResponse);
        handleConfigurationResponse(configResponse);
    }

    public ListenableFuture<ProcessResponse> process(ProcessRequest request) {
        if (closed) {
            return Futures.immediateFailedFuture(disconnectedException());
        }

        if (requestObserver == null) {
            connect();
        }

        SettableFuture<ProcessResponse> resultFuture = SettableFuture.create();
        futures.put(request.getRequestId(), resultFuture);

        requestObserver.onNext(request);

        return resultFuture;
    }

    public void connect() {
        requestObserver = stub.process(new StreamObserver<>() {
            @Override
            public void onNext(ProcessResponse value) {
                Long requestId = value.getRequestId();
                if (futures.containsKey(requestId)) {
                    futures.get(requestId).set(value);
                    futures.remove(requestId);
                }

                throw new RuntimeException("Unknown response id received from processor");
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                close();
            }

            @Override
            public void onCompleted() {
                close();
            }
        });
    }

    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;
        requestObserver.onCompleted();
        futures.values().forEach(future -> future.setException(disconnectedException()));
        futures.clear();
    }

    private void handleConfigurationResponse(ConfigProtos.ConfigurationResponse response) {
        PluginTypes.ImageFormat imageFormatResponse =
                response.getVersion().getProcessorV3().getCapabilitiesList().stream()
                        .filter(capability -> capability.hasVideo()
                                || (capability.hasImagery()
                                        && capability.getImagery().hasTiled()))
                        .map(capability -> {
                            if (capability.hasVideo()) {
                                return capability.getVideo().getImageFormat();
                            } else if (capability.getImagery().hasRaw()) {
                                this.supportsRaw = true;
                                return PluginTypes.ImageFormat.TIFF;
                            } else {
                                return capability.getImagery().getTiled().getImageFormat();
                            }
                        })
                        .findAny()
                        .orElseThrow(
                                () -> new RuntimeException(
                                        "Configuration response did not include image format for video or imagery capabilities"));

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

    private Throwable disconnectedException() {
        return new RuntimeException("gRPC stream disconnected");
    }

    public void closeChannel() {
        channel.shutdown();
    }
}
