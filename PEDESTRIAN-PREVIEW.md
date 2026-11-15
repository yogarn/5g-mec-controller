# Imported pedestrian workload

The pedestrian deliverables from the sibling `5g-mec-placement-controller-pedestrian-preview` snapshot have been imported into this repository, including detector code, Dockerfile, model export tools, local model binaries, validation artifacts, campaign selection and offline planning support. The original snapshot is retained.

Start with `apps/pedestrian-detection/README.md` and `apps/pedestrian-detection/validation/REPORT.md`. Model binaries and generated model metadata remain ignored by Git; a fresh clone must export the model before building. The validation reports describe the original preview environment, not a cluster deployment from this repository.

`pedestrian-workload.patch` is retained as the original preview archive. Its changes are already integrated; do not apply it again. No image was pushed or workload deployed during this import.

The runner defaults to the face workload identity. The existing local face manifest currently references the pedestrian preview image; choose the pedestrian workload explicitly for its separate Kubernetes identity and config. See the application README for offline planning and build commands.
