FROM golang:1.21-alpine3.19 AS builder

ARG AES_KEY="********************************"

WORKDIR /

COPY . .

RUN apk add --update build-base && apk upgrade --available
RUN go build -ldflags "-X git.brobridge.com/gravity/gravity-adapter-postgres/pkg/adapter/service.aesKey=$AES_KEY" -o /gravity-adapter-postgres ./cmd/gravity-adapter-postgres/gravity-adapter-postgres.go

RUN cd /build/docker/pwd_encrypt && \
        go build -ldflags "-X main.aesKey=$AES_KEY" -o /pwd_encrypt

FROM alpine:3.19.2
WORKDIR /

RUN apk update && apk upgrade --available && apk add tzdata

COPY --from=builder /gravity-adapter-postgres /gravity-adapter-postgres
COPY --from=builder /pwd_encrypt /pwd_encrypt
COPY ./configs /configs
COPY ./settings/ /settings/
COPY ./build/docker/startup.sh /startup.sh

RUN mkdir /statestore && \
        chown -R 1001:0  /settings /configs /statestore /gravity-adapter-postgres /pwd_encrypt /startup.sh && \
        chmod 777 /settings/sources.json /configs/config.toml  && \
        chmod -R g+rwX /statestore

USER 1001
ENV TZ="Asia/Taipei"

CMD ["sh", "/startup.sh"]
