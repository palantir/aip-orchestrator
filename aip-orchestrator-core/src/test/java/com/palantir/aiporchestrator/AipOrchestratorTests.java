/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.aiporchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class AipOrchestratorTests {

    @Test
    public void testMessage() {
        // If this is red in IntelliJ, click `Build, Build Project` to run the immutables annotation processor
        AipOrchestrator actual = ImmutableAipOrchestrator.builder()
                .recipient("someone")
                .message("Hello, world!")
                .build();

        assertThat(actual.message()).isEqualTo("Hello, world!");
    }
}
