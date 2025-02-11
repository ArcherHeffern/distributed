package main

import (
	"context"
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
	// handle_read_d(n)
	// handle_broadcast_d(n)
	// handle_topology_d(n)
	// handle_yap_d(n)

	// === 3e ===
	// handle_read_e(n)
	// handle_broadcast_e(n)
	// handle_topology_e(n)
	// batch_routine(n)

	// === 4 ===
	var kv = maelstrom.NewSeqKV(n)
	handle_add(kv, n)
	handle_read(kv, n)

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
// Challenge 3d: Efficient Multi Node Broadcast Service
// ############
// Issue: Probabilistic gossip protocol usually dropped 2-4 messages and it wasn't efficient enough
// Solution: Recipient can broadcast to every other node
//
// Issue: Very high 99-100% percentile latencies.
// Solution: Needed to use a lock. I was losing a lot of time on synchronization issues.
//
// Issue: Needed to maintain reliability of program
// Solution: Retry RPC up to 100 times using a backoff algorithm
//
// Other Notes:
// Others solution used hierarchical structure where you visit all chidren and parents, and don't backtrack to source or itself. Works but not necessary. (I'm still unsure why they implemented this)
var seen_map = make(map[float64]struct{})
var m sync.Mutex
var retry = 100

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
				rpcWithRetry(n, dest, req_body, retry)
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
// Challenge 3e: Efficient Multi Node Broadcast Service
// ############
// Constraints
// * Messages-per-operation is below 20
// * Median latency is below 1 second
// * Maximum latency is below 2 seconds
// Approach
// We now batch broadcast messages "yaps" at an interval to decrease overall network bandwidth
// Issue: msgs-per-opt actually increased to roughly 200
// Solution: Create a 1 deep hierarchical structure. This makes the top node very "Hot", but decreased msgs-per-opt to 3.7 (WOW)
var broadcast_mutex sync.Mutex
var to_broadcast = make([]float64, 0)
var batch_interval = 500 * time.Millisecond

func handle_read_e(n *maelstrom.Node) {
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

func handle_broadcast_e(n *maelstrom.Node) {
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

		var new_messages = make([]float64, 0)
		if message, exists := req_body["message"]; exists {
			new_messages = append(new_messages, float64(message.(float64)))
		}

		if messages, exists := req_body["messages"]; exists {
			for _, new_message := range messages.([]any) {
				new_messages = append(new_messages, float64(new_message.(float64)))
			}
		}

		m.Lock()
		broadcast_mutex.Lock()
		for _, new_message := range new_messages {
			if _, exists := seen_map[new_message]; !exists {
				to_broadcast = append(to_broadcast, new_message)
			}
			seen_map[new_message] = struct{}{}
		}
		broadcast_mutex.Unlock()
		m.Unlock()
		return nil
	})
}

func batch_rpc(n *maelstrom.Node) {
	broadcast_mutex.Lock()
	defer broadcast_mutex.Unlock()
	if len(to_broadcast) == 0 {
		return
	}

	wg := sync.WaitGroup{}
	var dests = make([]string, 0)

	if n.ID() == n.NodeIDs()[0] {
		for _, id := range n.NodeIDs() {
			dests = append(dests, id)
		}
	} else {
		dests = append(dests, n.NodeIDs()[0])
	}

	for _, dest := range dests {
		dest := dest
		to_broadcast := to_broadcast
		wg.Add(1)
		go func() {
			rpcWithRetry(n, dest, map[string]any{
				"type":     "broadcast",
				"messages": to_broadcast,
			}, retry)
			wg.Done()
		}()
	}
	wg.Wait()
	to_broadcast = make([]float64, 0)
}

func handle_topology_e(n *maelstrom.Node) {
	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})
}

func batch_routine(n *maelstrom.Node) {
	go func() {
		for {
			select {
			case <-time.After(batch_interval):
				batch_rpc(n)
			}
		}
	}()

}

// ############
// Challenge 4: Grow Only Counter
// ############
const K = "KEY"

func handle_add(kv *maelstrom.KV, n *maelstrom.Node) {
	n.Handle("add", func(msg maelstrom.Message) error {
		var req_body map[string]any
		if err := json.Unmarshal(msg.Body, &req_body); err != nil {
			return nil
		}
		var delta = int(req_body["delta"].(float64))
		for {
			var ctx1, cancel1 = context.WithTimeout(context.Background(), time.Second)
			defer cancel1()
			v, err := kv.ReadInt(ctx1, K)
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				v = 0
			} else if err != nil {
				continue
			}
			var ctx2, cancel2 = context.WithTimeout(context.Background(), time.Second)
			defer cancel2()
			if err := kv.CompareAndSwap(ctx2, K, v, v+delta, true); err == nil {
				break
			}
		}
		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})
}

func handle_read(kv *maelstrom.KV, n *maelstrom.Node) {
	n.Handle("read", func(msg maelstrom.Message) error {
		var value = 0
		var err error
		for {

			var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			value, err = kv.ReadInt(ctx, K)
			if err == nil {
				break
			}
		}
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
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

// Taken from https://github.com/teivah/gossip-glomers/blob/main/challenge-3e-broadcast/main.go
func rpcWithRetry(n *maelstrom.Node, dst string, body map[string]any, retry int) error {
	var err error
	for i := 0; i < retry; i++ {
		if err = rpc(n, dst, body); err != nil {
			time.Sleep(100 * time.Duration(i) * time.Millisecond)
			continue
		}
		return nil
	}
	return err
}

// Taken from https://github.com/teivah/gossip-glomers/blob/main/challenge-3e-broadcast/main.go
func rpc(n *maelstrom.Node, dst string, body map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := n.SyncRPC(ctx, dst, body)
	return err
}
