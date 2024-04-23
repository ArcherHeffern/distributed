#!/usr/bin/env bash

set -euo pipefail
# Exit on error
# Exit on unset value
# Exit on pipe failure

# On each additional problem: Add to the problems array and the switch statement

PROBLEMS=("Echo Server" "Unique Id Generation")
M="./maelstrom/maelstrom"
B="./challenges/node"

i=1
for problem in $PROBLEMS; do
	echo "${i}: ${problem}"
	((i++))
done

read -p "> " choice

$(
	DIR=$(dirname $B)
	BASE=$(basename $B)
	cd $DIR
	go build "${BASE}.go"
)


case $choice in
	1)
	$M test -w echo --bin $B --node-count 1 --time-limit 10;;	
	2) 
	$M test -w unique-ids --bin $B --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition;;	
esac
