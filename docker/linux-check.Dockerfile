# Validation environment only; never a published runtime image.
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e
RUN apt-get update && apt-get install -y --no-install-recommends \
    bubblewrap jq nodejs && rm -rf /var/lib/apt/lists/* \
    && rustup component add --toolchain 1.98.1 rustfmt clippy \
    && useradd --create-home --uid 1000 checker \
    && mkdir -p /work /cache/cargo /cache/target \
    && chown -R checker:checker /work /cache
ENV CARGO_HOME=/cache/cargo CARGO_TARGET_DIR=/cache/target CARGO_BUILD_JOBS=2
USER checker
WORKDIR /work
CMD ["bash", "scripts/linux-check.sh"]
