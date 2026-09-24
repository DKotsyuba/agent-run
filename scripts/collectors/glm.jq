include "common";
# Convert GLM Coding Plan windows; monthly MCP allowances are not inference quota.
(if .data != null then .data else . end).limits
| if type != "array" then error("missing quota limits") else . end
| [ .[] |
    if .type == "TIME_LIMIT" then empty
    elif .type == "TOKENS_LIMIT" or .type == "CREDIT_LIMIT" then
      . as $item
      | (if .type == "TOKENS_LIMIT" and .unit == null and .number == null then "five_hour"
         elif .unit == 3 and .number == 5 then "five_hour"
         elif .unit == 6 and .number == 1 then "seven_day"
         else error("unknown quota window") end) as $window
      | (if .percentage == null then null else (.percentage | percent) end) as $reported
      | (if .usage != null or .currentValue != null then
           if (.usage | type) != "number" or (.currentValue | type) != "number"
              or .usage <= 0 or .currentValue < 0 or .currentValue > .usage
           then error("invalid usage counts") else (.currentValue * 100 / .usage) end
         else null end) as $counted
      | if $reported != null and $counted != null and (($reported - $counted) | fabs) > 1
        then error("usage counts disagree") else . end
      | ($reported // $counted // error("missing usage")) as $used
      | (if .nextResetTime == null then null
         elif (.nextResetTime | type) == "number" and .nextResetTime > 0 and .nextResetTime <= 253402300799000
         then .nextResetTime / 1000 else error("invalid reset") end) as $reset
      | {pool:"primary", window:$window, models:($ctx.models | keys), remaining_percent:(100-$used),
         reset_at:$reset, observed_at:$ctx.now}
    else error("unknown quota type") end
  ] | envelope
