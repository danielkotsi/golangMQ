FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /broker ./cmd/broker

FROM alpine:3.19
COPY --from=builder /broker /broker
EXPOSE 5672 9090
CMD ["/broker"]
