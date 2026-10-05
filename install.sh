#!/bin/sh
# Install an attested macOS release; sealed-directory deploy remains unchanged.
set -eu
umask 077
# Fail before switching any installed version.
fail() { printf 'agent-run install: %s\n' "$*" >&2; exit 1; }
# Describe trusted tools and explicit filesystem destinations.
usage() {
    cat <<'HELP'
Usage: sh install.sh [--version X.Y.Z] [--prefix DIR] [--home DIR] [--bin-dir DIR]
                     [--downloader gh|curl|wget]
Requires gh (authenticated for private releases), jq, tar and a SHA-256 tool.
Only macOS Apple silicon is qualified. Stop the broker before updating.
Attestation verifies the exact repository/workflow/source before executable use.
Older releases lacking this manifest/provenance are refused by this bootstrap;
the existing sealed-directory deploy/rollback tooling remains compatible.
HELP
}
version=latest
install_home=${AGENT_RUN_HOME:-"$HOME/.agent-run"}
prefix=
bin_dir=$HOME/.local/bin
downloader=gh
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
case "$downloader" in gh|curl|wget) ;; *) fail '--downloader must be gh, curl or wget' ;; esac
for tool in gh jq tar ps "$downloader"; do command -v "$tool" >/dev/null 2>&1 || fail "required trusted tool: $tool"; done
if command -v shasum >/dev/null 2>&1; then hash_tool=shasum;
elif command -v sha256sum >/dev/null 2>&1; then hash_tool=sha256sum;
else fail 'shasum or sha256sum is required'; fi
temporary=$(mktemp -d "${TMPDIR:-/tmp}/agent-run-install.XXXXXXXX")
# Owned child slots and best-effort local ps identities; never displayed.
tool_pid= tool_identity= timer_pid= timer_identity= cleanup_unconfirmed=0
# process_identity PID prints its PPID/birth/command string, or empty if absent.
process_identity() { LC_ALL=C ps -p "$1" -o stat= -o ppid= -o lstart= -o args= 2>/dev/null | LC_ALL=C awk '$1 !~ /^Z/ { $1=""; sub(/^ /,""); print }'; }
# signal_owned PID IDENTITY SIGNAL signals only an unchanged captured identity.
# Empty/mismatching identity refuses signalling; this is a local best-effort guard.
signal_owned() {
    [ -n "$2" ] && [ "$(process_identity "$1")" = "$2" ] || return 1
    kill -"$3" "$1" 2>/dev/null
}
# child_terminated PID recognizes a zombie or observed absence. A live process
# with unavailable identity is still live, so callers refuse unconfirmed cleanup.
child_terminated() {
    child_state=$(LC_ALL=C ps -p "$1" -o stat= 2>/dev/null || :)
    case "$child_state" in *Z*) return 0 ;; "") ! kill -0 "$1" 2>/dev/null ;; *) return 1 ;; esac
}
# stop_owned PID IDENTITY terminates, briefly waits, hard-escalates and reaps
# this shell's child. A live unknown identity returns failure without unsafe kill.
stop_owned() {
    [ -n "$1" ] || return 0
    if signal_owned "$1" "$2" TERM; then
        for grace in 1 2 3 4 5 6 7 8 9 10; do
            child_terminated "$1" && break
            sleep 0.1
        done
        if ! child_terminated "$1"; then
            signal_owned "$1" "$2" KILL || return 1
        fi
    elif ! child_terminated "$1"; then
        return 1
    fi
    wait "$1" 2>/dev/null || :
}
# Stop/reap both child slots on EXIT/cancellation; preserve scratch if ownership
# could not be confirmed rather than claiming cleanup or signalling a foreign PID.
cleanup() {
    trap - EXIT INT TERM HUP USR1
    stop_owned "$tool_pid" "$tool_identity" || cleanup_unconfirmed=1
    stop_owned "$timer_pid" "$timer_identity" || cleanup_unconfirmed=1
    [ ! -f "$temporary/cleanup-unconfirmed" ] || cleanup_unconfirmed=1
    if [ "$cleanup_unconfirmed" = 0 ]; then rm -rf -- "$temporary";
    else printf 'agent-run install: child cleanup unconfirmed; scratch retained\n' >&2; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
trap 'cleanup_unconfirmed=1; fail "child identity unavailable/changed; cleanup unconfirmed"' USR1
# Bound trusted external tooling with exact-identity TERM/KILL escalation. The
# watchdog also owns/reaps its sleep; cancellation unwinds through cleanup.
bounded() {
    (ulimit -f 524288; if [ "$1" = gh ]; then GH_HOST=github.com; export GH_HOST; fi; exec "$@") & tool_pid=$!
    tool_identity=$(process_identity "$tool_pid" || :)
    parent_identity=$(process_identity "$$" || :)
    (
        # Reap only this watchdog's captured sleep child on controlled shutdown.
        sleep_pid= sleep_identity=
        timer_cleanup() {
            trap - TERM INT HUP
            if [ -n "$sleep_pid" ]; then
                signal_owned "$sleep_pid" "$sleep_identity" TERM || :
                if ! child_terminated "$sleep_pid"; then
                    signal_owned "$sleep_pid" "$sleep_identity" KILL || { : > "$temporary/cleanup-unconfirmed"; exit 1; }
                fi
                wait "$sleep_pid" 2>/dev/null || :
            fi
            exit 0
        }
        trap timer_cleanup TERM INT HUP
        sleep 180 & sleep_pid=$!
        sleep_identity=$(process_identity "$sleep_pid" || :)
        wait "$sleep_pid"
        sleep_pid=
        if signal_owned "$tool_pid" "$tool_identity" TERM; then
            sleep 1 & sleep_pid=$!
            sleep_identity=$(process_identity "$sleep_pid" || :)
            wait "$sleep_pid"
            sleep_pid=
            if ! child_terminated "$tool_pid"; then
                signal_owned "$tool_pid" "$tool_identity" KILL || signal_owned "$$" "$parent_identity" USR1
            fi
        elif ! child_terminated "$tool_pid"; then
            signal_owned "$$" "$parent_identity" USR1
        fi
    ) & timer_pid=$!
    timer_identity=$(process_identity "$timer_pid" || :)
    result=0
    wait "$tool_pid" || result=$?
    tool_pid= tool_identity=
    stop_owned "$timer_pid" "$timer_identity" || { cleanup_unconfirmed=1; fail 'watchdog cleanup unconfirmed'; }
    timer_pid= timer_identity=
    return "$result"
}
# Fetch fixed official assets; no user URL, token argument, overwrite or fallback.
download() {
    name=$1
    maximum=$2
    case "$downloader" in
        gh) bounded gh release download "v$version" --repo DKotsyuba/agent-run --pattern "$name" --dir "$temporary" ;;
        curl) bounded curl -q --fail --location --silent --show-error --proto '=https' --proto-redir '=https' --connect-timeout 15 --max-time 180 --max-filesize "$maximum" --output "$temporary/$name" "$base/$name" ;;
        wget) bounded wget --no-config --quiet --timeout=30 --tries=1 --output-document="$temporary/$name" "$base/$name" ;;
    esac || fail "download failed: $name"
    [ "$(wc -c < "$temporary/$name" | tr -d ' ')" -le "$maximum" ] || fail "download byte limit: $name"
}
# Hash exact bytes, retaining only the digest from the trusted hash tool.
sha256() {
    case "$hash_tool" in shasum) shasum -a 256 "$1" ;; sha256sum) sha256sum "$1" ;; esac | awk '{print $1}'
}
if [ "$version" = latest ]; then
    bounded gh release view --repo DKotsyuba/agent-run --json tagName,isDraft,isPrerelease > "$temporary/latest.json" || fail 'release lookup failed'
    [ "$(wc -c < "$temporary/latest.json")" -le 65536 ] || fail 'release lookup byte limit'
    version=$(jq -er 'select(.isDraft == false and .isPrerelease == false) | .tagName | ltrimstr("v")' "$temporary/latest.json") || fail 'stable release required'
fi
printf '%s\n' "$version" | LC_ALL=C awk 'BEGIN { ok=0 } /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/ { ok++ } END { exit !(NR == 1 && ok == 1) }' || fail 'version must be canonical X.Y.Z'
asset=agent-run-$version-aarch64-apple-darwin.tar.gz
base=https://github.com/DKotsyuba/agent-run/releases/download/v$version
bounded gh release view "v$version" --repo DKotsyuba/agent-run --json tagName,isDraft,isPrerelease > "$temporary/published.json" || fail 'exact release lookup failed'
[ "$(wc -c < "$temporary/published.json")" -le 65536 ] || fail 'release metadata byte limit'
jq -e --arg tag "v$version" '.tagName == $tag and .isDraft == false and .isPrerelease == false' "$temporary/published.json" >/dev/null || fail 'published stable exact release required'
bounded gh api --hostname github.com repos/DKotsyuba/agent-run > "$temporary/repo.json" || fail 'repository access failed'
[ "$(wc -c < "$temporary/repo.json")" -le 65536 ] || fail 'repository metadata limit'
jq -e '.id == 1348534205 and .full_name == "DKotsyuba/agent-run"' "$temporary/repo.json" >/dev/null || fail 'repository identity mismatch'
download release-manifest.json 65536
download SHA256SUMS 4096
jq -e --arg version "$version" --arg asset "$asset" '
    (keys | sort) == (["schema_version","product","version","tag","repository","repository_id","commit","workflow","standard_version","devkit_version","baseline","trust_profile","artifacts"] | sort) and
    .schema_version == 1 and .product == "agent-run" and .version == $version and .tag == ("v"+$version) and .repository == "DKotsyuba/agent-run" and .repository_id == 1348534205 and .trust_profile == "github-attestation" and (.commit | test("^[0-9a-f]{40}$")) and
    (.workflow | (keys | sort) == (["id","path","run_id","run_attempt"] | sort) and .id == 346709607 and .path == ".github/workflows/release.yml" and .run_id > 0 and .run_attempt > 0 and (.run_id | floor) == .run_id and (.run_attempt | floor) == .run_attempt) and
    all(.standard_version,.devkit_version,.baseline; type == "string" and length > 0 and length <= 128) and
    (.artifacts | length) == 4 and ([.artifacts[].kind] | sort) == (["bundle","source","installer","evidence"] | sort) and
    all(.artifacts[]; (.sha256 | test("^[0-9a-f]{64}$")) and .size > 0 and .size <= 536870912 and (.size | floor) == .size and
      if .kind == "bundle" then (keys | sort) == (["name","kind","target","size","sha256"] | sort) and .name == $asset and .target == "aarch64-apple-darwin"
      else (keys | sort) == (["name","kind","size","sha256"] | sort) and (.name == (if .kind == "source" then "agent-run-"+$version+"-source.tar" elif .kind == "installer" then "install.sh" else "acceptance.json" end)) end)
' "$temporary/release-manifest.json" >/dev/null || fail 'release manifest identity/layout mismatch'
commit=$(jq -r '.commit' "$temporary/release-manifest.json")
bounded gh api --hostname github.com "repos/DKotsyuba/agent-run/git/ref/tags/v$version" > "$temporary/tag.json" || fail 'tag lookup failed'
[ "$(wc -c < "$temporary/tag.json")" -le 65536 ] || fail 'tag metadata limit'
[ "$(jq -r '.object.type' "$temporary/tag.json")" = tag ] || fail 'annotated tag required'
for depth in 1 2 3 4 5; do
    object=$(jq -er '.object.sha | select(test("^[0-9a-f]{40}$"))' "$temporary/tag.json") || fail 'tag object invalid'
    bounded gh api --hostname github.com "repos/DKotsyuba/agent-run/git/tags/$object" > "$temporary/tag.json" || fail 'annotated tag lookup failed'
    [ "$(wc -c < "$temporary/tag.json")" -le 65536 ] || fail 'tag metadata limit'
    kind=$(jq -r '.object.type' "$temporary/tag.json")
    [ "$kind" != commit ] || break
    [ "$kind" = tag ] || fail 'tag target invalid'
done
[ "$kind" = commit ] && [ "$(jq -r '.object.sha' "$temporary/tag.json")" = "$commit" ] || fail 'tag/source identity mismatch'
manifest_expected=$(awk '$2 == "release-manifest.json" { print $1 }' "$temporary/SHA256SUMS")
[ "${#manifest_expected}" -eq 64 ] && [ "$(sha256 "$temporary/release-manifest.json")" = "$manifest_expected" ] || fail 'manifest checksum mismatch'
jq -r '.artifacts[] | "\(.sha256)  \(.name)"' "$temporary/release-manifest.json" > "$temporary/expected-sums"
printf '%s  release-manifest.json\n' "$manifest_expected" >> "$temporary/expected-sums"
cmp -s "$temporary/SHA256SUMS" "$temporary/expected-sums" || fail 'checksum mismatch/inventory conflict'
expected=$(jq -r --arg asset "$asset" '.artifacts[] | select(.name == $asset) | .sha256' "$temporary/release-manifest.json")
size=$(jq -r --arg asset "$asset" '.artifacts[] | select(.name == $asset) | .size' "$temporary/release-manifest.json")
[ "$(awk -v asset="$asset" '$2 == asset { print $1 }' "$temporary/SHA256SUMS")" = "$expected" ] || fail 'archive checksum mismatch'
download "$asset" "$size"
[ "$(wc -c < "$temporary/$asset" | tr -d ' ')" = "$size" ] && [ "$(sha256 "$temporary/$asset")" = "$expected" ] || fail 'archive size/checksum mismatch'
# Cryptographic provenance is enforced before extraction/executable use.
bounded gh attestation verify "$temporary/release-manifest.json" --hostname github.com --repo DKotsyuba/agent-run --signer-workflow DKotsyuba/agent-run/.github/workflows/release.yml --signer-digest "$commit" --source-digest "$commit" --source-ref "refs/tags/v$version" --deny-self-hosted-runners --limit 10 || fail 'manifest attestation verification failed; no trust downgrade'
bounded gh attestation verify "$temporary/$asset" --hostname github.com --repo DKotsyuba/agent-run --signer-workflow DKotsyuba/agent-run/.github/workflows/release.yml --signer-digest "$commit" --source-digest "$commit" --source-ref "refs/tags/v$version" --deny-self-hosted-runners --limit 10 || fail 'attestation verification failed; no trust downgrade'
# Reject normalized duplicates/paths and bounded native layout before extraction.
bounded tar -tzf "$temporary/$asset" > "$temporary/names" || fail 'invalid archive'
[ "$(wc -c < "$temporary/names")" -le 1048576 ] || fail 'archive metadata byte limit'
LC_ALL=C awk '
    /[^A-Za-z0-9_.\/-]/ || /^\// { exit 1 }
    { n=split($0,parts,"/"); for(i=1;i<=n;i++) if(parts[i]=="..") exit 1; name=$0; while(sub(/^\.\//,"",name)){}; sub(/\/$/,"",name); if(name==".")name=""; if(name!=""){ n=split(name,parts,"/");for(i=1;i<=n;i++)if(parts[i]=="."||parts[i]=="")exit 1 }; if(seen[name]++)exit 1; if(NR>10000)exit 1 }
    END { if(NR==0)exit 1 }
' "$temporary/names" || fail 'unsafe/duplicate archive member path/count'
bounded tar -tvzf "$temporary/$asset" > "$temporary/types" || fail 'invalid archive'
[ "$(wc -c < "$temporary/types")" -le 4194304 ] || fail 'archive listing byte limit'
LC_ALL=C awk '
    substr($0,1,1)!="-" && substr($0,1,1)!="d" { exit 1 }
    { mode=$1; if(mode !~ /^[-d][r-][w-][x-][r-]-[x-][r-]-[x-]$/)exit 1; size=($3~/^[0-9]+$/)?$3:$5; if(size!~/^[0-9]+$/)exit 1; total+=size; if(total>2147483648||NR>10000)exit 1;
      name=$NF; sub(/^\.\//,"",name); if(substr(mode,1,1)=="-"&&mode~/x/&&name!~/^(bin\/(agent-run|agent-run-tui|agent-run-deploy)|collectors\/[A-Za-z0-9_.-]+)$/)exit 1 }
' "$temporary/types" || fail 'archive links and special files, modes or unpacked bounds refused'
mkdir "$temporary/release"
bounded tar -xzf "$temporary/$asset" --no-same-owner --no-same-permissions -C "$temporary/release" || fail 'archive extraction failed'
helper=$temporary/release/bin/agent-run-deploy
[ -f "$helper" ] && [ -x "$helper" ] || fail 'verified release lacks its standalone installer'
# Downloaded programs receive no release-token environment.
unset GH_TOKEN GITHUB_TOKEN GH_ENTERPRISE_TOKEN GITHUB_ENTERPRISE_TOKEN AGENT_RUN_WORKER_TOKEN
"$helper" install --release "$temporary/release" --prefix "$prefix" --home "$install_home" --bin-dir "$bin_dir" --version "$version"
printf 'Ensure %s is on PATH, then run agent-run doctor.\n' "$bin_dir"
