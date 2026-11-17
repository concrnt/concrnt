FROM golang:latest AS core-builder
WORKDIR /work

ARG VERSION

COPY ./go.mod ./go.sum ./
RUN go mod download && go mod verify
COPY ./ ./
RUN VERSION=${VERSION} \
 && BUILD_MACHINE=$(uname -srmo) \
 && BUILD_TIME=$(date) \
 && GO_VERSION=$(go version) \
 && go build -ldflags "-s -w -X main.version=${VERSION} -X \"main.buildMachine=${BUILD_MACHINE}\" -X \"main.buildTime=${BUILD_TIME}\" -X \"main.goVersion=${GO_VERSION}\"" -o concrnt ./cmd/concrnt

# ---------------------------------
FROM golang:latest AS cmd-builder
WORKDIR /work

ARG VERSION

COPY ./go.mod ./go.sum ./
RUN go mod download && go mod verify
COPY ./ ./
RUN VERSION=${VERSION} \
 && BUILD_MACHINE=$(uname -srmo) \
 && BUILD_TIME=$(date) \
 && GO_VERSION=$(go version) \
 && go build -ldflags "-s -w -X main.version=${VERSION} -X \"main.buildMachine=${BUILD_MACHINE}\" -X \"main.buildTime=${BUILD_TIME}\" -X \"main.goVersion=${GO_VERSION}\"" -o conctl ./cmd/conctl


# ---------------------------------
FROM ubuntu:latest AS certificates

RUN apt-get update && apt-get install -y ca-certificates

# ---------------------------------
FROM ubuntu:latest

COPY --from=core-builder /work/concrnt /usr/local/bin
COPY --from=cmd-builder /work/conctl /usr/local/bin
COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

CMD ["concrnt"]

