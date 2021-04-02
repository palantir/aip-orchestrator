/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aip.processing;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.util.function.BiConsumer;

public final class GrpcCalls {
    private GrpcCalls() {}

    public static <RequestT, ResponseT> ListenableFuture<ResponseT> call(
            BiConsumer<RequestT, StreamObserver<ResponseT>> stubCall, RequestT request) {
        SettableFuture<ResponseT> response = SettableFuture.create();

        stubCall.accept(request, new StreamObserver<>() {
            @Override
            public void onNext(ResponseT value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable throwable) {
                response.setException(throwable);
            }

            @Override
            public void onCompleted() {}
        });

        return response;
    }
}
