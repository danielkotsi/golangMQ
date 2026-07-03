FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o /broker ./cmd/broker

FROM alpine:3.19
COPY --from=builder /broker /broker
EXPOSE 5672
CMD ["/broker"]
