# Sources
- [Fly.io](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)

"A series of distributed systems challenges brought to you by fly.io"

# Set Up
1. Install go
2. Install Maelstrom into the parent directory

# Running
```bash
./run.sh
```

# Testing
```bash
(cd challenges && go build node.go) && cat ./tests/1 | ./challenges/node
```
