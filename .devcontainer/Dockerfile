# Use Ubuntu as the base image (pinned to specific version for reproducibility)
FROM golangci/golangci-lint:v2.7.2

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install basic dependencies (including gvm requirements)
RUN apt-get update && apt-get install -y \
    curl \
    git \
    mercurial \
    make \
    binutils \
    bison \
    gcc \
    build-essential \
    ca-certificates \
    bsdmainutils \
    zsh \
    locales \
    && rm -rf /var/lib/apt/lists/*

# Configure locales to prevent bash warnings
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen en_US.UTF-8

# Set locale environment variables
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LC_COLLATE=en_US.UTF-8

# Install gvm (Go Version Manager)
RUN curl -s -S -L https://raw.githubusercontent.com/moovweb/gvm/master/binscripts/gvm-installer -o /tmp/gvm-installer.sh \
    && bash /tmp/gvm-installer.sh \
    && rm /tmp/gvm-installer.sh

# Source gvm and install Go 1.25.5 as default version
RUN /bin/bash -c "source /root/.gvm/scripts/gvm && \
    echo 'Installing Go 1.25.5...' && \
    gvm install go1.25.5 -B && \
    gvm use go1.25.5 --default && \
    echo 'Go 1.25.5 installed and set as default'"

# Set up Go environment (gvm handles PATH, but we set other vars)
ENV GOPATH="/go"
ENV GOPROXY="direct"
ENV GOSUMDB="off"

# Add gvm to PATH for all users
ENV PATH="/root/.gvm/bin:${PATH}"

# Create go workspace directory
RUN mkdir -p /go

# Install common Go tools with pinned versions (gvm should be in PATH from previous step)
# Versions pinned as of early 2026 for reproducibility
# To check latest versions:
#   - gopls: go list -m -versions golang.org/x/tools/gopls
#   - delve: https://github.com/go-delve/delve/releases
#   - staticcheck: https://github.com/dominikh/go-tools/releases
#   - gotests: https://github.com/cweill/gotests/releases
#   - impl: https://github.com/josharian/impl/releases
#   - goplay: https://github.com/haya14busa/goplay/releases
#   - goimports: go list -m -versions golang.org/x/tools/cmd/goimports
RUN /bin/bash -c "source /root/.gvm/scripts/gvm && \
    go install golang.org/x/tools/gopls@v0.21.0 && \
    go install github.com/go-delve/delve/cmd/dlv@v1.22.0 && \
    go install honnef.co/go/tools/cmd/staticcheck@v0.6.1 && \
    go install github.com/cweill/gotests/gotests@v1.6.0 && \
    go install github.com/josharian/impl@v1.2.0 && \
    go install github.com/haya14busa/goplay/cmd/goplay@v1.0.0 && \
    go install golang.org/x/tools/cmd/goimports@v0.21.0 && \
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2"

# Initialize gvm in bash and zsh profiles for interactive shells
RUN echo 'source /root/.gvm/scripts/gvm' >> /root/.bashrc

# Set zsh as default shell
RUN chsh -s $(which zsh) root || true

# Set working directory
WORKDIR /workspace

# Copy scripts at the end to preserve layer caching
# Changes to these scripts won't invalidate earlier layers
COPY .devcontainer/setup-zsh.sh /usr/local/bin/setup-zsh.sh
RUN chmod +x /usr/local/bin/setup-zsh.sh

