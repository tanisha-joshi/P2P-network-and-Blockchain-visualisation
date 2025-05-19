from typing import Dict, List, Optional
import asyncio
import aiohttp
import json
from .node import BlockchainNode

class NetworkManager:
    def __init__(self):
        self.nodes: Dict[str, BlockchainNode] = {}
        self.shard_nodes: Dict[int, List[str]] = {}
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        """Initialize the network manager"""
        self.session = aiohttp.ClientSession()

    async def stop(self):
        """Clean up network resources"""
        if self.session:
            await self.session.close()

    def register_node(self, node: BlockchainNode):
        """Register a new node in the network"""
        self.nodes[node.node_id] = node
        if node.shard_id not in self.shard_nodes:
            self.shard_nodes[node.shard_id] = []
        self.shard_nodes[node.shard_id].append(node.node_id)

    async def broadcast_transaction(self, transaction: Dict, shard_id: int):
        """Broadcast a transaction to all nodes in a shard"""
        if shard_id not in self.shard_nodes:
            return

        for node_id in self.shard_nodes[shard_id]:
            node = self.nodes[node_id]
            await self._send_transaction(node, transaction)

    async def _send_transaction(self, node: BlockchainNode, transaction: Dict):
        """Send a transaction to a specific node"""
        try:
            # In a real implementation, this would be an HTTP request to the node's API
            node.add_transaction(transaction)
        except Exception as e:
            print(f"Error sending transaction to node {node.node_id}: {e}")

    async def sync_shard(self, shard_id: int):
        """Synchronize all nodes in a shard"""
        if shard_id not in self.shard_nodes:
            return

        for node_id in self.shard_nodes[shard_id]:
            node = self.nodes[node_id]
            await node.sync_with_peers()

    def get_shard_nodes(self, shard_id: int) -> List[str]:
        """Get all node IDs in a shard"""
        return self.shard_nodes.get(shard_id, [])

    async def cross_shard_communication(self, from_shard: int, to_shard: int, message: Dict):
        """Handle communication between shards"""
        # This would implement the cross-shard communication protocol
        # In a real implementation, this would involve:
        # 1. Validating the message
        # 2. Creating a cross-shard transaction
        # 3. Broadcasting to the target shard
        pass 