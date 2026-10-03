# ---- build ----
FROM golang:1.27-alpine AS build

WORKDIR /src

# Кэш зависимостей
COPY go.mod go.sum ./
RUN go mod tidy
# RUN go mod vendor

# Сборка
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /out/app ./main.go

# ---- runtime ----
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata wget && \
    adduser -D -u 10001 app

WORKDIR /app

COPY --from=build /out/app /app/app

USER app

EXPOSE 8080

ENTRYPOINT ["/app/app"]
