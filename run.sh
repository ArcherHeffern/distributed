#!/usr/bin/env bash

set -euo pipefail
# Exit on error
# Exit on unset value
# Exit on pipe failure

# On each additional problem: Add to the problems array and the switch statement

M="./maelstrom/maelstrom"
B="./challenges/node"

PROBLEMS=('Echo Server' 'Unique Id Generation' 'Single Node Broadcast' 'Multi Node Broadcast' 'Fault Tolerant Broadcast' 'Efficient Broadcast I' 'Efficient Broadcast II')

i=1
for problem in "${PROBLEMS[@]}"; do
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
	$M test -w unique-ids --bin $B --time-limit 3 --rate 1000 --node-count 3 --availability total --nemesis partition;;	
	3)
	$M test -w broadcast --bin $B --node-count 1 --time-limit 20 --rate 10;;
	4) 
	$M test -w broadcast --bin $B --node-count 5 --time-limit 20 --rate 10;;
	5)
	$M test -w broadcast --bin $B --node-count 5 --time-limit 20 --rate 10 --nemesis partition;;
	6) 
	$M test -w broadcast --bin $B --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition;;
	7)
	$M test -w broadcast --bin $B --node-count 25 --time-limit 20 --rate 100 --latency 100;;
	*)
		echo "Unrecognized option";;
esac
