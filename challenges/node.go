package main

import (
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/leonelquinteros/gorand"
)

func main() {
	n := maelstrom.NewNode()
	// 1
	// handle_echo(n)
	// 2
	// handle_generate_id(n)
	// 3a
	// handle_read_a(n)
	// handle_broadcast_a(n)
	// handle_topology_a(n)
	// 3a
	handle_read_a(n)
	handle_broadcast_a(n)
	handle_topology_a(n)
	// 3b-c
	handle_read_b(n)
	handle_broadcast_b(n)
	handle_topology_b(n)
	// 4
	// 5

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

// ############
// Challenge 1: Echo Service
// ############
func handle_echo(n *maelstrom.Node) {
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "echo_ok"
		return n.Reply(msg, body)
	})
}

// ############
// Challenge 2: UUID Service
// ############
func handle_generate_id(n *maelstrom.Node) {
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		t := time.Now().UnixNano()
		uuid, err := gorand.UUIDv4()
		if err != nil {
			panic(err.Error())
		}
		out := fmt.Sprintf("%d%s", t, uuid)
		body["type"] = "generate_ok"
		body["id"] = out
		return n.Reply(msg, body)
	})
}

// ############
// Challenge 3a: Single Node Broadcast Service
// ############
var seen = make([]float64, 0)

func handle_read_a(n *maelstrom.Node) {
	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var req_body map[string]any
		var res_body = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return err
		}
		res_body["type"] = "read_ok"
		res_body["messages"] = seen
		return n.Reply(msg, res_body)
	})
}

func handle_broadcast_a(n *maelstrom.Node) {
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var req_body map[string]any
		var res_body = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return err
		}
		res_body["type"] = "broadcast_ok"
		seen = append(seen, req_body["message"].(float64))
		return n.Reply(msg, res_body)
	})
}

func handle_topology_a(n *maelstrom.Node) {
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var req_body map[string]any
		var res_body = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return err
		}
		res_body["type"] = "topology_ok"
		return n.Reply(msg, res_body)
	})
}

// ############
// Challenge 3b-c: Multi Node Broadcast Service
// ############
const FANOUT = 3

func handle_read_b(n *maelstrom.Node) {
	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var req_body map[string]any
		var res_body = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return err
		}
		res_body["type"] = "read_ok"
		res_body["messages"] = seen
		return n.Reply(msg, res_body)
	})
}

func handle_broadcast_b(n *maelstrom.Node) {
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var req_body map[string]any
		var res_body = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return err
		}
		var message = req_body["message"].(float64)
		// If we have already seen this, don't send out
		if !slices.Contains(seen, message) {
			for _, dest := range n.NodeIDs() {
				if dest == n.ID() {
					continue
				}
				n.Send(dest, req_body)
			}
			seen = append(seen, message)
		}

		res_body["type"] = "broadcast_ok"
		return n.Reply(msg, res_body)
	})
}

func handle_topology_b(n *maelstrom.Node) {
	n.Handle("topology", func(msg maelstrom.Message) error {
		var res_body = make(map[string]any)
		res_body["type"] = "topology_ok"
		return n.Reply(msg, res_body)
	})
}
