FROM golang:1.24-alpine AS build
RUN apk add --no-cache gcc musl-dev
WORKDIR /src
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/order-consumer ./cmd/order-consumer && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rank-consumer ./cmd/rank-consumer && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/outbox-publisher ./cmd/outbox-publisher && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/release-worker ./cmd/release-worker

FROM build AS test
CMD ["go", "test", "./..."]

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/gateway /app/gateway
COPY --from=build /out/order-consumer /app/order-consumer
COPY --from=build /out/rank-consumer /app/rank-consumer
COPY --from=build /out/outbox-publisher /app/outbox-publisher
COPY --from=build /out/release-worker /app/release-worker
USER nonroot:nonroot
EXPOSE 8080
