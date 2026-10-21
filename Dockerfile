FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tollgate ./cmd/tollgate

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tollgate /tollgate
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/tollgate"]
