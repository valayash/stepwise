FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/stepwise ./cmd/stepwise

# distroless: no shell, no package manager, runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/stepwise /stepwise
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/stepwise"]
# `docker run -p 8080:8080 stepwise` runs the in-memory demo; use `serve`, `api` or `worker` in production.
CMD ["dev"]
