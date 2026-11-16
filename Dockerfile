# Builds the Helix image ghcr.io/helix-nextgen-cellular-core/sctplb:v1.0.0
# the same way it was built and verified:
#   docker build -t sctplb:local .
FROM golang:1.18.3-stretch AS builder
WORKDIR /go/src/sctplb
COPY . .
RUN go mod vendor \
 && make all && mkdir -p /out/sdcore/bin && cp bin/* /out/sdcore/bin/

FROM alpine:3.17
LABEL org.opencontainers.image.source="https://github.com/Helix-NextGen-Cellular-Core/sctplb" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN apk update && apk add -U vim strace net-tools curl netcat-openbsd bind-tools bash libc6-compat
COPY --from=builder /out/ /
WORKDIR /sdcore
