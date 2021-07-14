FROM alpine:3.10

ENV OPERATOR_CONFIG=/usr/local/bin/operator_config.yaml \
    USER_UID=1001 \
    USER_NAME=serverlessdb-operator \
    TZ=Asia/Shanghai

COPY operator_config.yaml /usr/local/bin/operator_config.yaml

COPY bin /usr/local/bin
COPY entrypoint /usr/local/bin
COPY user_setup /usr/local/bin

RUN apk --no-cache add tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone
RUN  /usr/local/bin/user_setup

ENTRYPOINT ["/usr/local/bin/entrypoint"]

USER ${USER_UID}
