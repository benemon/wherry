# docker build -f docs/agent.Containerfile --build-arg CLAUDE_CODE_VERSION=<version> -t claude-agent:dev .
# wherry dev up --agent-image claude=claude-agent:dev
FROM docker.io/library/node:22-bookworm-slim@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da
ARG CLAUDE_CODE_VERSION
RUN test -n "$CLAUDE_CODE_VERSION" && npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" && npm cache clean --force
USER node
