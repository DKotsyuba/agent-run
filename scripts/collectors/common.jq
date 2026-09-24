# Validate one provider percentage without manufacturing unknown values.
def percent:
  if type == "number" and . >= 0 and . <= 100 then . else error("invalid percentage") end;

# Tokenize a provider model spelling for exact, case-insensitive membership.
def tokens: ascii_downcase | [scan("[a-z0-9]+")];

# Parse an optional RFC3339 reset, including fractional seconds and UTC offsets.
def reset:
  if . == null then null
  elif type != "string" then error("invalid reset")
  else (capture("^(?<date>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\\.(?<fraction>[0-9]+))?(?<zone>Z|[+-][0-9]{2}:[0-9]{2})$") // error("invalid reset")) as $t
    | ($t.date + "Z" | fromdateiso8601) as $base
    | (if $t.zone == "Z" then 0 else
        ($t.zone[1:3] | tonumber) as $h | ($t.zone[4:6] | tonumber) as $m
        | if $h > 23 or $m > 59 then error("invalid offset")
          else (($h * 3600 + $m * 60) * (if $t.zone[0:1] == "-" then -1 else 1 end)) end
       end) as $offset
    | $base - $offset + (if ($t.fraction // "") == "" then 0 else ("0." + $t.fraction | tonumber) end)
  end;

# Require at least one window and wrap it in the public collector output contract.
def envelope:
  if length == 0 then error("no applicable quota windows") else {version:1, windows:.} end;
