FROM golang:alpine AS build-env
COPY . /work
WORKDIR /work
RUN go get -u
RUN go build

FROM alpine
COPY --from=build-env /work/provider_bus_operator /sxbin/provider_bus_operator
WORKDIR /sxbin
