AIP Orchestrator
================

This repo contains a light-weight simulator of the AI Inference Platform (AIP). It can send InferenceRequests to a processor at some frames-per-second, and print the InferenceResponses received.

The real AIP is Palantir Gotham’s proprietary platform to perform real-time augmentations and computations on streamed videos. AIP is the base platform that accepts video streams as input, and decodes them. Then, it runs the decoded video through zero or more “processor” services, which are responsible for actually processing the video.
