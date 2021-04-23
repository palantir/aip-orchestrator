AIP Orchestrator
================

This repo contains a light-weight simulator of the AI Inference Platform (AIP). It can send InferenceRequests to a processor at some frames-per-second, and print the InferenceResponses received.

The real AIP is Palantir Gotham’s proprietary platform to perform real-time augmentations and computations on streamed videos. AIP is the base platform that accepts video streams as input, and decodes them. Then, it runs the decoded video through zero or more “processor” services, which are responsible for actually processing the video. Please read the [AIP SDK Documentation](https://palantir.github.io/aip-sdk/introduction) for more information.

The orchestrator comes with a sample image of dimensions 2048 x 2048 x 3, that it sends to the processor in either of the RGB888, BGR888, PNG or TIFF formats (depending on what the processor specifies in the configuration). It currently does not support using custom images, as the prime goal of the orchestrator is to test that a processor can establish basic communication with the AIP interface. To run your own images/video through, please use AIP.

Running the Orchestrator
========================

Please refer to the [QuickStart Guide](https://palantir.github.io/aip-sdk/quickstart) for how to run this orchestrator along with a sample Python inference processor.
