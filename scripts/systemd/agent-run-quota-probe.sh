#!/usr/bin/env bash
# Run the configured quota probe as the owner of the selected native login.
set -euo pipefail
umask 077

directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
context=$(cat)
kind=$(printf '%s' "$context" | jq -er '.auth.kind')
fence=$(printf '%s' "$context" | jq -ce '.fence')
case "$kind" in
    codex_native)
        printf '%s' "$context" |
            jq -c --arg home "$HOME/.codex" '.auth = {kind:"native_login",directory:$home}' |
            timeout --kill-after=3s 35s /bin/bash "$directory/codex.sh" |
            jq -c --argjson fence "$fence" '{fence:$fence,observation:.}'
        ;;
    claude_named)
        label=$(printf '%s' "$context" | jq -er '.auth.label | select(type == "string")')
        [[ $label =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || exit 1
        credential="$HOME/.agent-run/accounts/claude/$label/claude-config/.credentials.json"
        [[ -f $credential && ! -L $credential ]] || exit 1
        token=$(jq -er '(.claudeAiOauth.accessToken // .accessToken) |
            select(type == "string" and length > 0 and (test("[\u0000-\u001f\u007f]") | not))' \
            "$credential" 2>/dev/null)
        observation=$(printf '%s\n%s' "$context" "$token" |
            jq -Rsc 'split("\n") as $input | select(($input | length) == 2) |
                ($input[0] | fromjson) + {auth:{kind:"token",token:$input[1]}}' |
            timeout --kill-after=3s 35s /bin/bash "$directory/claude.sh" |
            head -c 2097153 | jq -ce '.')
        (( ${#observation} <= 2097152 )) || exit 1
        printf '%s\n%s' "$observation" "$token" |
            jq -Rsc --argjson fence "$fence" '
                def contains_secret($secret):
                    if type == "string" then contains($secret)
                    elif type == "array" then any(.[]; contains_secret($secret))
                    elif type == "object" then any(to_entries[];
                        (.key | contains($secret)) or (.value | contains_secret($secret)))
                    else false end;
                split("\n") as $input | select(($input | length) == 2) |
                ($input[0] | fromjson) as $observation |
                if ($observation | contains_secret($input[1])) then
                    error("quota output contains credential")
                else {fence:$fence,observation:$observation} end'
        ;;
    *) exit 1 ;;
esac
