# Adapter plugins

A receiver runs each task through an adapter, and each adapter type is a
plugin: a separate binary the receiver starts from its plugin directory and
talks to over the adapter protocol. The
[adapter plugins reference](reference.md#adapter-plugins) specifies what a
receiver expects of a plugin, and the protocol is the Go module in
[`adapter`](https://github.com/benemon/wherry/tree/main/adapter).

The plugins live in
[wherry-plugins](https://github.com/benemon/wherry-plugins), each with its
own README covering installation, settings and the platform's
permissions:

- [Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad)
