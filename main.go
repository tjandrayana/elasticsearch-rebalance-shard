package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	esHost             = "http://localhost:9200"
	rebalanceThreshold = 10 // Maximum allowed difference in shard count between nodes
	sleepInterval      = 60 * time.Second
)

type ClusterHealth struct {
	Status string `json:"status"`
}

type ClusterState struct {
	RoutingNodes struct {
		Nodes map[string][]interface{} `json:"nodes"`
	} `json:"routing_nodes"`
}

func getClusterHealth() (*ClusterHealth, error) {
	resp, err := http.Get(esHost + "/_cluster/health")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var health ClusterHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return nil, err
	}
	return &health, nil
}

func getClusterState() (*ClusterState, error) {
	resp, err := http.Get(esHost + "/_cluster/state/routing_nodes")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var state ClusterState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

func getShardDistribution(state *ClusterState) map[string]int {
	shardDistribution := make(map[string]int)
	for nodeID, shards := range state.RoutingNodes.Nodes {
		shardDistribution[nodeID] = len(shards)
	}
	return shardDistribution
}

func isBalanced(shardDistribution map[string]int) bool {
	var maxShards, minShards int
	for _, shardCount := range shardDistribution {
		if shardCount > maxShards {
			maxShards = shardCount
		}
		if minShards == 0 || shardCount < minShards {
			minShards = shardCount
		}
	}
	return (maxShards - minShards) <= rebalanceThreshold
}

func rebalanceShards() {
	fmt.Println("Rebalancing shards...")

	// Disable shard allocation temporarily
	disableAllocation()

	// Get current cluster state
	state, err := getClusterState()
	if err != nil {
		fmt.Println("Error getting cluster state:", err)
		enableAllocation()
		return
	}

	fmt.Printf("[xxx] state : %v\n", state)

	shardDistribution := getShardDistribution(state)

	// Determine if the cluster is already balanced
	if isBalanced(shardDistribution) {
		fmt.Println("Cluster is already balanced.")
		enableAllocation()
		return
	}

	// Move shards to balance the cluster
	for nodeID, shardCount := range shardDistribution {
		if shardCount > rebalanceThreshold {
			// Get the node with the fewest shards
			targetNodeID := minShardNode(shardDistribution)

			// Move a shard from the overloaded node to the target node
			moveShard(nodeID, targetNodeID)
			time.Sleep(5 * time.Second) // Give some time for the move to complete
		}
	}

	enableAllocation()
}

func minShardNode(shardDistribution map[string]int) string {
	var minNode string
	minShards := -1
	for nodeID, shardCount := range shardDistribution {
		if minShards == -1 || shardCount < minShards {
			minShards = shardCount
			minNode = nodeID
		}
	}
	return minNode
}

func disableAllocation() {
	fmt.Println("Disabling shard allocation...")
	settings := map[string]interface{}{
		"transient": map[string]interface{}{
			"cluster.routing.allocation.enable": "none",
		},
	}
	sendClusterSettings(settings)
}

func enableAllocation() {
	fmt.Println("Enabling shard allocation...")
	settings := map[string]interface{}{
		"transient": map[string]interface{}{
			"cluster.routing.allocation.enable": nil,
		},
	}
	sendClusterSettings(settings)
}

func moveShard(sourceNode, targetNode string) {
	fmt.Printf("Moving shard from node %s to node %s...\n", sourceNode, targetNode)
	settings := map[string]interface{}{
		"transient": map[string]interface{}{
			"cluster.routing.allocation.exclude._name": sourceNode,
			"cluster.routing.allocation.include._name": targetNode,
		},
	}
	sendClusterSettings(settings)
}

func sendClusterSettings(settings map[string]interface{}) {
	jsonData, err := json.Marshal(settings)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	req, err := http.NewRequest("PUT", esHost+"/_cluster/settings", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return
	}

	fmt.Println("Response:", string(body))
}

func main() {
	for {
		rebalanceShards()
		time.Sleep(sleepInterval)
	}
}
