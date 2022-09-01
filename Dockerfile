FROM golang:1.16-alpine as builder

RUN apk add --no-cache \
    make \
    git \
    bash \
    curl \
    gcc \
    g++ \
    openssh

# Setup ssh key for private deps
ARG ssh_key
RUN if [ -n "$ssh_key" ]; then \
        mkdir -p ~/.ssh && \
        echo "$ssh_key" > ~/.ssh/key && \
        chmod 600 ~/.ssh/key && \
        echo "Host github.com" >> ~/.ssh/config && \
        echo "\tUser git" >> ~/.ssh/config && \
        echo "\tPort 443" >> ~/.ssh/config && \
        echo "\tHostName ssh.github.com" >> ~/.ssh/config && \
        echo "\tIdentityFile ~/.ssh/key" >> ~/.ssh/config && \
        ssh-keyscan -p 443 ssh.github.com>> ~/.ssh/known_hosts && \
        git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"; \
    fi

# Install jq for pd-ctl
RUN cd / && \
    wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 -O jq && \
    chmod +x jq

RUN mkdir -p /go/src/github.com/tikv/pd
WORKDIR /go/src/github.com/tikv/pd

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN make

FROM alpine:3.5

COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-server /pd-server
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-ctl /pd-ctl
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-recover /pd-recover
COPY --from=builder /jq /usr/local/bin/jq

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
