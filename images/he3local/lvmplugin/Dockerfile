FROM centos:7.3.1611
LABEL maintainers="Kubernetes Authors"
LABEL description="HostPath Driver"
ARG binary=./bin/lvmplugin
ARG mc=./mc
# Add util-linux to get a new version of losetup.
RUN yum install lvm2 -y
RUN yum install file -y
RUN yum install -y e4fsprogs
RUN yum install -y xfsprogs
COPY ${binary} /hostpathplugin
COPY ${mc} //usr/bin/mc
ENTRYPOINT ["/hostpathplugin"]
