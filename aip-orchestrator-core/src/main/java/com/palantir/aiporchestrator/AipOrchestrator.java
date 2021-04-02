/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aiporchestrator;

import org.immutables.value.Value;

/**
 * When you compile this class (using './gradlew classes' or 'Build', 'Build Project' in IntelliJ), the <a
 * href="https://immutables.github.io/">immutables</a> annotation processor will generate an implementation of this
 * interface with correct builder, hashCode and toString methods.
 */
@Value.Immutable
public interface AipOrchestrator {

    String recipient();

    String message();
}
