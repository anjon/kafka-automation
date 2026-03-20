FROM jenkins/jenkins:lts
USER root

# 1. Install prerequisites (including curl and gnupg)
RUN apt-get update && apt-get install -y \
    lsb-release \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates

# 2. Add Docker’s official GPG key
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# 3. Set up the repository
RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
    $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list

# 4. Install the Docker CLI
RUN apt-get update && apt-get install -y docker-ce-cli

USER jenkins