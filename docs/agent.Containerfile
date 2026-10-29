# An agent image for wherry dev up --agent-image claude=IMAGE. The dev profile runs
# /bin/sh -c 'mkdir -p "$WHERRY_OUTPUT_DIR" && cd /local && exec claude -p ... < prompt.txt'
# in it, so the image needs /bin/sh and claude on PATH. The run's credential reaches it as
# CLAUDE_CODE_OAUTH_TOKEN; no login is baked in.
#
#   docker build -f docs/agent.Containerfile --build-arg CLAUDE_CODE_VERSION=<version> -t claude-agent:dev .
#   wherry dev up --agent-image claude=claude-agent:dev
FROM docker.io/library/node:22-bookworm-slim
ARG CLAUDE_CODE_VERSION
RUN test -n "$CLAUDE_CODE_VERSION" && npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" && npm cache clean --force
USER node
