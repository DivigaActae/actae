#!/bin/sh
# Actae installer.
#
#   curl -fsSL https://raw.githubusercontent.com/DivigaActae/actae/main/install.sh | sh
#
# Environment:
#   ACTAE_VERSION   tag to install (default: latest release)
#   INSTALL_DIR     destination directory (default: ~/.local/bin)
#   ACTAE_NO_VERIFY set to 1 to skip checksum verification (not recommended)
set -eu

REPO="DivigaActae/actae"
BIN="actae"

say() { printf '%s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
need uname
need curl
need tar

os=$(uname -s)
arch=$(uname -m)

case "$os" in
  Darwin)
    case "$arch" in
      arm64) target="aarch64-apple-darwin" ;;
      x86_64) target="x86_64-apple-darwin" ;;
      *) die "unsupported macOS architecture: $arch" ;;
    esac ;;
  Linux)
    case "$arch" in
      aarch64|arm64) target="aarch64-unknown-linux-gnu" ;;
      x86_64|amd64) target="x86_64-unknown-linux-gnu" ;;
      *) die "unsupported Linux architecture: $arch" ;;
    esac ;;
  *) die "unsupported operating system: $os" ;;
esac

version="${ACTAE_VERSION:-}"
if [ -z "$version" ]; then
  version=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)
  [ -n "$version" ] || die "no published release found; build from source instead:
  git clone https://github.com/${REPO}.git && cd actae && cargo build --release"
fi

asset="${BIN}-${version}-${target}.tar.gz"
base="https://github.com/${REPO}/releases/download/${version}"

say "Installing ${BIN} ${version} for ${target}"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

curl -fsSL "${base}/${asset}" -o "${tmp}/${asset}" \
  || die "download failed: ${base}/${asset}"

if [ "${ACTAE_NO_VERIFY:-0}" != "1" ]; then
  sums="${tmp}/SHA256SUMS-${target}"
  if curl -fsSL "${base}/SHA256SUMS-${target}" -o "$sums"; then
    expected=$(awk -v f="$asset" '$2==f || $2=="*"f {print $1}' "$sums" | head -1)
    if [ -n "$expected" ]; then
      if command -v sha256sum >/dev/null 2>&1; then
        actual=$(sha256sum "${tmp}/${asset}" | awk '{print $1}')
      else
        actual=$(shasum -a 256 "${tmp}/${asset}" | awk '{print $1}')
      fi
      [ "$actual" = "$expected" ] || die "checksum mismatch for ${asset}"
      say "Checksum verified"
    else
      say "warning: ${asset} missing from checksum file; continuing"
    fi
  else
    say "warning: no checksum file for ${target}; continuing"
  fi
fi

tar -xzf "${tmp}/${asset}" -C "$tmp"
[ -f "${tmp}/${BIN}" ] || die "archive did not contain ${BIN}"
chmod +x "${tmp}/${BIN}"

dest="${INSTALL_DIR:-$HOME/.local/bin}"
mkdir -p "$dest"
if [ -w "$dest" ]; then
  mv "${tmp}/${BIN}" "${dest}/${BIN}"
else
  die "cannot write to ${dest}; set INSTALL_DIR to a writable directory"
fi

say "Installed to ${dest}/${BIN}"
case ":${PATH}:" in
  *":${dest}:"*) ;;
  *) say "Add ${dest} to PATH:  export PATH=\"${dest}:\$PATH\"" ;;
esac
say "Next:  ${BIN}    (opens http://localhost:8002, no account required)"
