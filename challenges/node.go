package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/leonelquinteros/gorand"
)

func main() {
	n := maelstrom.NewNode()
	// === 1 ===
	// handle_echo(n)

	// === 2 ===
	// handle_generate_id(n)

	// === 3a ===
	// handle_read_a(n)
	// handle_broadcast_a(n)
	// handle_topology_a(n)

	// === 3b-c ===
	// handle_read_b(n)
	// handle_broadcast_b(n)
	// handle_topology_b(n)

	// === 3d ===
	handle_read_d(n)
	handle_broadcast_d(n)
	handle_topology_d(n)
	handle_yap_d(n)

	// === 4 ===

	// === 5 ===

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

// ############
// Challenge 3d: Single Node Broadcast Service
// ############
// I tried to implement a gossip protocol here, but 2-4 messages were usually were dropped. In addition, this was not efficient enough to be accepted.
// Solution: Can broadcast to each other node
// Issue: Needed to use a lock. I was losing a lot of time on synchronization issues.
// Others solution uses hierarchical structure where you visit all chidren and parents, and don't backtrack to source or itself. Works but not necessary for performance.
var seen_map = make(map[float64]struct{})
var m sync.Mutex

func handle_read_d(n *maelstrom.Node) {
	n.Handle("read", func(msg maelstrom.Message) error {
		m.Lock()
		var seen_list []float64
		for k, _ := range seen_map {
			seen_list = append(seen_list, k)
		}
		m.Unlock()
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": seen_list,
		})
	})
}

func handle_broadcast_d(n *maelstrom.Node) {
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var req_body map[string]any
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return err
		}
		go func() {
			n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}()

		var message = req_body["message"].(float64)
		m.Lock()
		seen_map[message] = struct{}{}
		m.Unlock()
		req_body["type"] = "yap"
		for _, dest := range n.NodeIDs() {
			if dest == n.ID() {
				continue
			}
			go func() {
				n.RPC(dest, req_body, nil)
			}()
		}

		return nil
	})
}

func handle_yap_d(n *maelstrom.Node) {
	n.Handle("yap", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var req_body map[string]any
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return err
		}
		var message = req_body["message"].(float64)
		m.Lock()
		seen_map[message] = struct{}{}
		m.Unlock()
		return nil
	})
}

func handle_topology_d(n *maelstrom.Node) {
	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})
}

// ############
// UTILS
// ############
func select_n_random(list []string, n int) []string {
	if n > len(list) {
		// If n exceeds the list size, return the entire list (or handle error).
		n = len(list)
	}

	// Shuffle the copied list in place.
	rand.Shuffle(len(list), func(i, j int) {
		list[i], list[j] = list[j], list[i]
	})

	// Return the first n elements from the shuffled copy.
	return list[:n]
}
