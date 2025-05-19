import asyncio
import sys
from web3 import Web3
from .node import BlockchainNode

async def start_node(node_id: str, shard_id: int, host: str, port: int, bootstrap_nodes: list):
    """Start a single blockchain node"""
    # Connect to local Ethereum node
    web3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
    
    # Wait for connection to be established
    while not web3.is_connected():
        print(f"Waiting for connection to Ethereum node...")
        await asyncio.sleep(1)
    
    print(f"Connected to Ethereum node. Starting {node_id}...")
    node = BlockchainNode(node_id, shard_id, web3, host, port)
    
    try:
        await node.start()
        await node.discover_peers(bootstrap_nodes)
        
        # Start mining
        while True:
            await node.mine_block()
            await asyncio.sleep(10)  # Mine every 10 seconds
            
    except KeyboardInterrupt:
        await node.stop()
    except Exception as e:
        print(f"Error in node {node_id}: {e}")
        await node.stop()

async def main():
    """Start multiple nodes in different shards"""
    # Configuration for nodes
    nodes_config = [
        {
            "node_id": "node_0_0",
            "shard_id": 0,
            "host": "127.0.0.1",
            "port": 5000
        },
        {
            "node_id": "node_0_1",
            "shard_id": 0,
            "host": "127.0.0.1",
            "port": 5001
        },
        {
            "node_id": "node_1_0",
            "shard_id": 1,
            "host": "127.0.0.1",
            "port": 5002
        },
        {
            "node_id": "node_1_1",
            "shard_id": 1,
            "host": "127.0.0.1",
            "port": 5003
        }
    ]

    # Bootstrap nodes (first node in each shard)
    bootstrap_nodes = [
        f"127.0.0.1:5000",  # First node in shard 0
        f"127.0.0.1:5002"   # First node in shard 1
    ]

    print("Starting blockchain nodes...")
    print("Press Ctrl+C to stop all nodes")

    # Start all nodes
    tasks = []
    for config in nodes_config:
        task = asyncio.create_task(
            start_node(
                config["node_id"],
                config["shard_id"],
                config["host"],
                config["port"],
                bootstrap_nodes
            )
        )
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\nShutting down nodes...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main()) 