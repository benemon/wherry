# Install the wherry CLI

Install the `wherry` CLI on each machine you run it on.
For a deployment, that is the operator machine and your laptop.
For a development instance, it is the one machine that runs the instance.

## Install wherry

Install wherry with [Homebrew](https://brew.sh/), on macOS or Linux:

```sh
brew install benemon/tap/wherry
```

Homebrew installs the archive for your operating system and architecture.
It checks the archive against the formula's SHA-256.
The formula follows prereleases until the first stable release tag exists.
From then on, it follows stable releases only.

::: details Install from a release archive instead
Each [release](https://github.com/benemon/wherry/releases) has an archive per OS and architecture, `checksums.txt`, and
a signature over the checksums. Verify the signature with
[cosign](https://docs.sigstore.dev/cosign/system_config/installation/), then the archive against the checksums:

```sh
version=v0.1.0-rc.28 os=linux arch=amd64
base=https://github.com/benemon/wherry/releases/download/$version
archive=wherry_${version}_${os}_${arch}.tar.gz
curl -fsSL --remote-name-all "$base/$archive" "$base/checksums.txt" "$base/checksums.txt.sigstore.json"
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/benemon/wherry/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
grep "  $archive\$" checksums.txt | sha256sum -c -
tar -xzf "$archive" --strip-components=1 "${archive%.tar.gz}/wherry"
install -m 0755 wherry /usr/local/bin/wherry
```

On macOS, use `shasum -a 256 -c -` in place of `sha256sum -c -`.
:::

## Check that it worked

```sh
wherry version
```

It prints the release you installed as `version=<release>`, and its commit as `revision=<commit>`.
