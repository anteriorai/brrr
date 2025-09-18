#!/usr/bin/env bash

set -euo pipefail

count="$(git rev-list @ --count)"
<package.json \
	jq \
	--arg gc "$count" \
    '. * { version : "\(.version | split(".")[:2] | join(".")).\($gc)"}' \
    | sponge package.json
