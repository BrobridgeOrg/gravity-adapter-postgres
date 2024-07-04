FROM golang:1.21-alpine3.19 AS builder

WORKDIR /

COPY . .

RUN apk add --update build-base && apk upgrade --available
RUN go build -o /gravity-adapter-postgres ./cmd/gravity-adapter-postgres/gravity-adapter-postgres.go


FROM alpine:3.19.2
WORKDIR /

RUN apk update && apk upgrade --available && apk add tzdata

COPY --from=builder /gravity-adapter-postgres /gravity-adapter-postgres
COPY ./configs /configs
COPY ./settings/ /settings/
COPY ./build/docker/startup.sh /startup.sh

RUN mkdir /statestore && \
        chown -R 1001:0  /settings /configs /statestore /gravity-adapter-postgres /startup.sh && \
        chmod 777 /settings/sources.json /configs/config.toml  && \
        chmod -R g+rwX /statestore

USER 1001
ENV TZ="Asia/Taipei"

CMD ["sh", "/startup.sh"]