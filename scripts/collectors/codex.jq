include "common";
# Name a native quota window consistently with previously stored observations.
def window_name:
  if . == 300 then "five_hour" elif . == 10080 then "seven_day" else "min\(.)" end;

# Dedicated model buckets constrain only exact token-matching native model names.
(.result // .) as $response
| (if ($response.rateLimitsByLimitId | type) == "object" then $response.rateLimitsByLimitId
   elif ($response.rateLimits | type) == "object" then
     {($response.rateLimits.limitId // "codex"): $response.rateLimits}
   else error("missing rate-limit buckets") end) as $buckets
| ($ctx.models | keys) as $models
| [ $buckets | to_entries[] | select(.key != "codex") | . as $bucket
    | (.value.limitName // .key | tokens) as $name
    | {id:.key, models:[$models[] | select((tokens) == $name)]}
    | select((.models | length) > 0)
  ] as $dedicated
| ($dedicated | map(.models[]) | unique) as $scoped
| ($dedicated + (if $buckets | has("codex") then [{id:"codex",models:($models-$scoped)}] else [] end))
| [ .[] | select((.models | length) > 0) | . as $membership
    | $buckets[.id] | .primary, .secondary | select(. != null)
    | (.usedPercent | percent) as $used
    | if (.windowDurationMins | type) != "number" or .windowDurationMins <= 0
      then error("invalid window duration") else . end
    | if .resetsAt != null and ((.resetsAt | type) != "number" or .resetsAt < 0 or .resetsAt > 253402300799)
      then error("invalid reset") else . end
    | {pool:$membership.id, window:(.windowDurationMins | window_name), models:$membership.models,
       remaining_percent:(100-$used), reset_at:.resetsAt, observed_at:$ctx.now}
  ] | envelope
