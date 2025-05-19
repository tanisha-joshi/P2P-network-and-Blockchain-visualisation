from typing import List, Dict
from dataclasses import dataclass
from web3 import Web3
import hashlib

@dataclass
class Shard:
    id: int
    nodes: List[str]
    current_block: Dict
    state: Dict

class ShardManager:
    def __init__(self, num_shards: int):
        self.shards: Dict[int, Shard] = {}
        self.num_shards = num_shards
        self._initialize_shards()
        self.node_count = 0  # Track number of nodes for round-robin assignment

    def _initialize_shards(self):
        """Initialize the shards with empty state"""
        for i in range(self.num_shards):
            self.shards[i] = Shard(
                id=i,
                nodes=[],
                current_block={},
                state={}
            )

    def assign_node_to_shard(self, node_id: str) -> int:
        """Assign a node to a shard using round-robin distribution"""
        shard_id = self.node_count % self.num_shards
        self.shards[shard_id].nodes.append(node_id)
        self.node_count += 1
        return shard_id

    def get_shard_for_vote(self, vote_id: str) -> int:
        """Determine which shard should handle a specific vote using consistent hashing"""
        return int(hashlib.sha256(vote_id.encode()).hexdigest(), 16) % self.num_shards

    def get_shard_state(self, shard_id: int) -> Dict:
        """Get the current state of a shard"""
        return self.shards[shard_id].state

    def update_shard_state(self, shard_id: int, new_state: Dict):
        """Update the state of a specific shard"""
        self.shards[shard_id].state = new_state

    def get_all_shard_states(self) -> Dict[int, Dict]:
        """Get the state of all shards"""
        return {shard_id: shard.state for shard_id, shard in self.shards.items()} 