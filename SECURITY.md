# Security policy

## Supported versions

Security fixes go into the latest release: wherry v0.1.x and the
`pluginsdk` module v0.9.x. Release candidates and older releases receive
none.

## Reporting a vulnerability

Report a vulnerability privately through
[GitHub private vulnerability reporting](https://github.com/benemon/wherry/security/advisories/new).
Do not open a public issue, pull request or discussion for it.

Include the wherry version (`wherry version`), the component (host, operator,
receiver, plugin SDK or MCP server), the steps that reproduce it, and what an
attacker gains. A vulnerability in a platform plugin belongs in
[wherry-plugins](https://github.com/benemon/wherry-plugins/security/advisories/new).

[Security](https://benemon.github.io/wherry/security) describes the
credential, signing and key custody model that reports are judged against.
