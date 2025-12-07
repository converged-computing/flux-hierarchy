FROM fluxrm/flux-sched:jammy

LABEL maintainer="Vanessasaurus <@vsoch>"

# Match the default user id for a single system so we aren't root
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000
ENV USERNAME=${USERNAME}
ENV USER_UID=${USER_UID}
ENV USER_GID=${USER_GID}
USER root
RUN apt-get update && apt-get install -y less python3-pip

# "Install" the frobnicator plugin
RUN wget https://raw.githubusercontent.com/researchapps/flux-core/add-dependency-frob-plugin/src/bindings/python/flux/job/frobnicator/plugins/dependency.py && \
    mv dependency.py /usr/lib/python3.10/site-packages/flux/job/frobnicator/plugins/

# Add the group and user that match our ids
RUN groupadd -g ${USER_GID} ${USERNAME} && \
    adduser --disabled-password --uid ${USER_UID} --gid ${USER_GID} --gecos "" ${USERNAME} && \
    echo "${USERNAME} ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers

USER $USERNAME
