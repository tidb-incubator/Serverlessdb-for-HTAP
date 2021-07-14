FROM centos:7.3.1611

RUN set -e; \
    rpm --import /etc/pki/rpm-gpg/RPM*; \
    yum clean all && yum provides '*/applydeltarpm'; yum install deltarpm -y; \
    yum install -y mysql bind-utils \
    yum clean all

COPY bin/scale-operator /scale-operator
RUN chmod a+x /scale-operator
EXPOSE 8888

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
ENTRYPOINT ["/scale-operator"]

CMD [""]