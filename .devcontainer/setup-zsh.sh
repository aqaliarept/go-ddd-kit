#!/bin/bash
# Setup script to configure zsh with user's settings from host

# Add safety settings at the top of .zshrc to prevent completion errors
# This must be at the top to catch errors from user's config
if [ -f /root/.zshrc ]; then
    # Check if safety header already exists
    if ! grep -q '# DevContainer safety settings' /root/.zshrc 2>/dev/null; then
        # Create temp file with safety settings + original content
        {
            echo '# DevContainer safety settings - prevent zsh completion errors'
            echo 'setopt NO_NOMATCH 2>/dev/null || true'
            echo 'unsetopt FUNCTION_ARGZERO 2>/dev/null || true'
            echo '# Disable problematic completion functions'
            echo 'unfunction _encode 2>/dev/null || true'
            echo 'autoload -Uz compinit 2>/dev/null || true'
            echo 'compinit -d /tmp/zcompdump 2>/dev/null || true'
            echo ''
            cat /root/.zshrc
        } > /root/.zshrc.tmp && mv /root/.zshrc.tmp /root/.zshrc
    fi
    
    # Ensure gvm is sourced
    if ! grep -q 'source /root/.gvm/scripts/gvm' /root/.zshrc; then
        echo '' >> /root/.zshrc
        echo '# Source gvm' >> /root/.zshrc
        echo 'source /root/.gvm/scripts/gvm 2>/dev/null || true' >> /root/.zshrc
    fi
else
    # Create default .zshrc if user's doesn't exist
    cat > /root/.zshrc << 'EOF'
# Prevent zsh completion errors in container
setopt NO_NOMATCH 2>/dev/null || true
unsetopt FUNCTION_ARGZERO 2>/dev/null || true

# Disable problematic completion functions that may cause errors
unfunction _encode 2>/dev/null || true
autoload -Uz compinit
compinit -d /tmp/zcompdump 2>/dev/null || true

# Source gvm
source /root/.gvm/scripts/gvm 2>/dev/null || true

# Go environment
export GOPATH="/go"
export GOPROXY="direct"
export GOSUMDB="off"
EOF
fi

# If oh-my-zsh is mounted but not installed, install it
if [ -d /root/.oh-my-zsh ] && [ ! -f /root/.oh-my-zsh/oh-my-zsh.sh ]; then
    echo "Oh My Zsh directory found but not properly installed"
fi

# Handle .zshenv - add safety wrapper if user's is mounted
if [ -f /root/.zshenv ]; then
    # If it's not our wrapper, create a safe version
    if ! grep -q '# DevContainer zshenv safety' /root/.zshenv 2>/dev/null; then
        # Backup and create safe wrapper
        cp /root/.zshenv /root/.zshenv.user 2>/dev/null || true
        {
            echo '# DevContainer zshenv safety - prevent errors'
            echo 'setopt NO_NOMATCH 2>/dev/null || true'
            echo 'unsetopt FUNCTION_ARGZERO 2>/dev/null || true'
            echo ''
            echo '# Source user zshenv safely'
            echo 'if [ -f /root/.zshenv.user ]; then'
            echo '    source /root/.zshenv.user 2>/dev/null || true'
            echo 'fi'
        } > /root/.zshenv
    fi
fi


echo "Zsh configuration complete"

