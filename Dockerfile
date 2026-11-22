# Shopping Sessions: native C++ pipeline.
# Stage 1 builds one static binary; stage 2 is an empty image holding only it,
# so the container starts with no interpreter, no shared libraries, no shell.
FROM debian:bookworm-slim AS build
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ \
 && rm -rf /var/lib/apt/lists/*
COPY src/ /src/
RUN set -eu; \
    case "${TARGETARCH:-amd64}" in \
      amd64) ARCH="-march=x86-64-v3 -mtune=cascadelake" ;; \
      arm64) ARCH="-mcpu=generic" ;; \
      *)     ARCH="" ;; \
    esac; \
    g++ -std=c++17 -O3 $ARCH -fno-plt -fomit-frame-pointer -pthread -static \
        -o /pipeline /src/main.cpp; \
    strip /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
# The scorer runs as uid 1000 with a read-only root; the binary needs neither
# a home nor any writable path besides /output.
USER 1000:1000
CMD ["/pipeline"]
