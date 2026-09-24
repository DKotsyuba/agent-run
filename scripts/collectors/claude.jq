include "common";
# Resolve a scoped weekly pool only to explicitly configured matching model names.
def scoped($models):
  if (.scope | type) != "object" or .scope.surface != null or (.scope.model | type) != "object"
  then error("unknown usage scope") else . end
  | .scope.model as $scope
  | if ($scope.id != null and ($scope.id | type) != "string")
       or ($scope.display_name != null and ($scope.display_name | type) != "string")
    then error("invalid model scope") else . end
  | ($scope.display_name // "" | tokens) as $wanted
  | (if ($scope.id // "") != "" then $scope.id
     elif ($wanted | length) > 0 then ($wanted | join("-")) else error("empty model scope") end) as $key
  | {pool:("model:"+$key), window:"seven_day", models:[$models[] |
      . as $name | (tokens) as $parts
      | select($name == $scope.id or (($wanted | length) > 0 and (($wanted - $parts) | length) == 0))]};

# Only limits[] is authoritative here; parallel summary objects are not added twice.
.limits
| if type != "array" then error("missing usage limits") else . end
| [ .[] | . as $entry
    | (if .kind == "session" or .kind == "weekly_all" then
         if .scope != null then error("unexpected scope") else
           {pool:(if .kind == "session" then "primary" else "secondary" end),
            window:(if .kind == "session" then "five_hour" else "seven_day" end), models:($ctx.models | keys)} end
       elif .kind == "weekly_scoped" then scoped($ctx.models | keys)
       else error("unknown usage kind") end) as $window
    | ($entry.percent | percent) as $used
    | if $entry.is_active != null and ($entry.is_active | type) != "boolean" then error("invalid active flag") else . end
    | ($entry.resets_at | reset) as $reset
    | select(($window.models | length) > 0)
    | $window + {remaining_percent:(100-$used), reset_at:$reset, observed_at:$ctx.now}
  ] | envelope
