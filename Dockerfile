FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stepwise ./cmd/stepwise

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/stepwise /stepwise
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/stepwise"]
# `docker run -p 8080:8080 stepwise dev` for the in-memory demo; `serve` in production.
CMD ["dev"]
