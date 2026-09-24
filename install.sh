#!/bin/sh
# Install a qualified GitHub release without Cargo or Python. Repeat to update.
set -eu
umask 077

# Print one actionable failure without switching an installed version.
fail() { printf 'agent-run install: %s\n' "$*" >&2; exit 1; }

# Describe the stable download and installation interface.
usage() {
    cat <<'HELP'
Usage: sh install.sh [--version X.Y.Z] [--prefix DIR] [--home DIR] [--bin-dir DIR]
                     [--downloader curl|wget]
Defaults: latest GitHub release, ~/.agent-run/standalone, ~/.agent-run, ~/.local/bin.
Only macOS Apple silicon is qualified. Stop the broker before updating.
Configuration, accounts and engine CLIs are not created or replaced.
HELP
}

version=latest
install_home=${AGENT_RUN_HOME:-"$HOME/.agent-run"}
prefix=
bin_dir=$HOME/.local/bin
downloader=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --version|--prefix|--home|--bin-dir|--downloader)
            [ "$#" -ge 2 ] || fail "missing value for $1"
            case "$1" in
                --version) version=${2#v} ;;
                --prefix) prefix=$2 ;;
                --home) install_home=$2 ;;
                --bin-dir) bin_dir=$2 ;;
                --downloader) downloader=$2 ;;
            esac
            shift 2 ;;
        *) fail "unknown argument: $1" ;;
    esac
done
prefix=${prefix:-"$install_home/standalone"}
for destination in "$prefix" "$install_home" "$bin_dir"; do
    case "$destination" in /*) ;; *) fail 'use absolute installation paths' ;; esac
    [ "$destination" != / ] || fail 'refusing filesystem root as installation directory'
done
[ "$(uname -s)/$(uname -m)" = Darwin/arm64 ] || fail 'only macOS Apple silicon (Darwin/arm64) is qualified'
if [ -z "$downloader" ]; then
    if command -v curl >/dev/null 2>&1; then downloader=curl; else downloader=wget; fi
fi
case "$downloader" in curl|wget) ;; *) fail '--downloader must be curl or wget' ;; esac
command -v "$downloader" >/dev/null 2>&1 || fail 'install curl or wget to download releases'
command -v tar >/dev/null 2>&1 || fail 'tar is required'
if command -v shasum >/dev/null 2>&1; then hash_tool=shasum;
elif command -v sha256sum >/dev/null 2>&1; then hash_tool=sha256sum;
else fail 'shasum or sha256sum is required'; fi

temporary=$(mktemp -d "${TMPDIR:-/tmp}/agent-run-install.XXXXXXXX")
# Remove only the private directory allocated by this invocation.
cleanup() { rm -rf -- "$temporary"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

# Fetch only fixed official HTTPS URLs; user curl/wget configuration cannot alter requests.
download() {
    case "$downloader" in
        curl) curl -q --fail --location --silent --show-error --proto '=https' --proto-redir '=https' \
            --connect-timeout 15 --max-time 180 --retry 2 --output "$2" "$1" ;;
        wget) wget --no-config --quiet --timeout=30 --tries=3 --output-document="$2" "$1" ;;
    esac || fail "download failed: $1"
}

if [ "$version" = latest ]; then
    download https://api.github.com/repos/DKotsyuba/agent-run/releases/latest "$temporary/latest.json"
    version=$(sed -n 's/^[[:space:]]*"tag_name"[[:space:]]*:[[:space:]]*"v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)".*/\1/p' "$temporary/latest.json")
fi
printf '%s\n' "$version" | LC_ALL=C awk 'BEGIN { ok=0 } /^[0-9]+\.[0-9]+\.[0-9]+$/ { ok++ } END { exit !(NR == 1 && ok == 1) }' || fail 'version must be X.Y.Z'
asset=agent-run-$version-aarch64-apple-darwin.tar.gz
base=https://github.com/DKotsyuba/agent-run/releases/download/v$version
printf 'Downloading agent-run %s…\n' "$version"
download "$base/SHA256SUMS" "$temporary/SHA256SUMS"
download "$base/$asset" "$temporary/release.tar.gz"
expected=$(awk -v asset="$asset" '$2 == asset { print $1 }' "$temporary/SHA256SUMS")
[ "${#expected}" -eq 64 ] || fail 'missing or duplicate archive checksum'
case "$expected" in *[!0-9a-fA-F]*) fail 'invalid archive checksum' ;; esac
case "$hash_tool" in
    shasum) actual=$(shasum -a 256 "$temporary/release.tar.gz") ;;
    sha256sum) actual=$(sha256sum "$temporary/release.tar.gz") ;;
esac
[ "${actual%% *}" = "$expected" ] || fail 'archive checksum mismatch'

# Refuse traversal, links and special files before tar can write any archive member.
tar -tzf "$temporary/release.tar.gz" > "$temporary/names" || fail 'invalid archive'
LC_ALL=C awk '
    /[^A-Za-z0-9_.\/-]/ || /^\// { exit 1 }
    { n=split($0, parts, "/"); for (i=1; i<=n; i++) if (parts[i] == "..") exit 1 }
    END { if (NR == 0) exit 1 }
' "$temporary/names" || fail 'unsafe archive member path'
tar -tvzf "$temporary/release.tar.gz" > "$temporary/types" || fail 'invalid archive'
LC_ALL=C awk 'substr($0,1,1) != "-" && substr($0,1,1) != "d" { exit 1 }' "$temporary/types" || fail 'archive links and special files are forbidden'
mkdir "$temporary/release"
tar -xzf "$temporary/release.tar.gz" --no-same-owner --no-same-permissions -C "$temporary/release"
helper=$temporary/release/bin/agent-run-deploy
[ -f "$helper" ] && [ -x "$helper" ] || fail 'this release predates the standalone installer; select a release containing bin/agent-run-deploy'
"$helper" install --release "$temporary/release" --prefix "$prefix" --home "$install_home" --bin-dir "$bin_dir" --version "$version"
printf 'Ensure %s is on PATH. Configure engines and accounts, then run agent-run doctor.\n' "$bin_dir"
