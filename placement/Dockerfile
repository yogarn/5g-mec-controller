FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/placement-controller ./cmd/placement-controller

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/placement-controller /placement-controller
ENTRYPOINT ["/placement-controller"]
CMD ["--config=/etc/placement/config.yaml"]
