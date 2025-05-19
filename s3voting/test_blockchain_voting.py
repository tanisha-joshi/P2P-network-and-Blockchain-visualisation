import asyncio
from s3voting.voting.vote_manager import VoteManager
from s3voting.blockchain.node import BlockchainNode
from s3voting.blockchain.shard import ShardManager
from web3 import Web3
import logging
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import time
import json
import hashlib
from datetime import datetime
from s3voting.blockchain.node import BlockchainNode as Node
from s3voting.blockchain.p2p import P2PProtocol
import random
from enum import Enum, auto

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElectionPhase(Enum):
    REGISTRATION = auto()
    VOTING = auto()
    TALLYING = auto()
    RESULTS = auto()

class ElectionManager:
    def __init__(self):
        self.current_phase = ElectionPhase.REGISTRATION
        self.phase_start_time = time.time()
        self.phase_durations = {
            ElectionPhase.REGISTRATION: 15,  # 15 seconds for registration
            ElectionPhase.VOTING: 25,        # 30 seconds for voting
            ElectionPhase.TALLYING: 10,      # 20 seconds for tallying
            ElectionPhase.RESULTS: 7        # 5 seconds for results
        }
        self.registered_voters = set()
        self.votes_cast = set()
        
    def is_phase_active(self, phase: ElectionPhase) -> bool:
        """Check if a specific phase is currently active"""
        return self.current_phase == phase
    
    def can_register_voter(self, voter_id: str) -> bool:
        """Check if voter registration is allowed"""
        return (self.is_phase_active(ElectionPhase.REGISTRATION) and 
                voter_id not in self.registered_voters)
    
    def can_cast_vote(self, voter_id: str) -> bool:
        """Check if voting is allowed for a specific voter"""
        return (self.is_phase_active(ElectionPhase.VOTING) and 
                voter_id in self.registered_voters and 
                voter_id not in self.votes_cast)
    
    def register_voter(self, voter_id: str) -> bool:
        """Register a voter if allowed"""
        if self.can_register_voter(voter_id):
            self.registered_voters.add(voter_id)
            return True
        return False
    
    def record_vote(self, voter_id: str) -> bool:
        """Record a vote if allowed"""
        if self.can_cast_vote(voter_id):
            self.votes_cast.add(voter_id)
            return True
        return False
    
    def should_transition_phase(self) -> bool:
        """Check if it's time to transition to the next phase"""
        current_duration = time.time() - self.phase_start_time
        return current_duration >= self.phase_durations[self.current_phase]
    
    def transition_to_next_phase(self) -> ElectionPhase:
        """Transition to the next phase"""
        if self.current_phase == ElectionPhase.REGISTRATION:
            self.current_phase = ElectionPhase.VOTING
        elif self.current_phase == ElectionPhase.VOTING:
            self.current_phase = ElectionPhase.TALLYING
        elif self.current_phase == ElectionPhase.TALLYING:
            self.current_phase = ElectionPhase.RESULTS
        self.phase_start_time = time.time()
        return self.current_phase

class PerformanceMetrics:
    def __init__(self):
        self.block_times = []  # Time taken to mine each block
        self.transaction_times = {}  # Time from vote submission to block inclusion
        self.start_time = None
        self.total_transactions = 0
        self.total_blocks = 0
        self.shard_metrics = {}  # Metrics per shard
        self.block_sizes = []  # Size of each block in transactions
        self.consensus_times = []  # Time taken for consensus
        self.network_latencies = []  # Network latency measurements
        
    def start_test(self):
        """Start the performance test"""
        self.start_time = time.time()
        
    def record_transaction_submission(self, voter_id):
        """Record when a transaction is submitted"""
        if voter_id not in self.transaction_times:
            self.transaction_times[voter_id] = {
                "submit_time": time.time(),
                "shard_id": None
            }
        
    def record_block_mined(self, block_hash, block_index, shard_id, num_transactions):
        """Record when a block is mined"""
        if self.start_time is None:
            return
        block_time = time.time() - self.start_time
        self.block_times.append((block_index, block_time))
        self.block_sizes.append(num_transactions)
        self.total_blocks += 1
        
        # Update shard metrics
        if shard_id not in self.shard_metrics:
            self.shard_metrics[shard_id] = {
                "blocks_mined": 0,
                "transactions_processed": 0,
                "avg_block_time": 0,
                "total_block_time": 0
            }
        
        self.shard_metrics[shard_id]["blocks_mined"] += 1
        self.shard_metrics[shard_id]["transactions_processed"] += num_transactions
        self.shard_metrics[shard_id]["total_block_time"] += block_time
        self.shard_metrics[shard_id]["avg_block_time"] = (
            self.shard_metrics[shard_id]["total_block_time"] / 
            self.shard_metrics[shard_id]["blocks_mined"]
        )
        
    def record_transaction_inclusion(self, voter_id, block_index, shard_id):
        """Record when a transaction is included in a block"""
        if voter_id in self.transaction_times and "submit_time" in self.transaction_times[voter_id]:
            submit_time = self.transaction_times[voter_id]["submit_time"]
            latency = time.time() - submit_time
            self.transaction_times[voter_id] = {
                "block_index": block_index,
                "latency": latency,
                "shard_id": shard_id
            }
            self.total_transactions += 1
            
    def record_consensus_time(self, consensus_time):
        """Record time taken for consensus"""
        self.consensus_times.append(consensus_time)
        
    def record_network_latency(self, latency):
        """Record network latency measurement"""
        self.network_latencies.append(latency)
            
    def get_metrics(self):
        """Get the performance metrics"""
        if self.start_time is None:
            return "Test not started"
            
        if not self.block_times:
            return "No blocks mined"
            
        total_time = time.time() - self.start_time
        avg_block_time = sum(t for _, t in self.block_times) / len(self.block_times)
        throughput = self.total_transactions / total_time if total_time > 0 else 0
        
        latencies = [t["latency"] for t in self.transaction_times.values() if "latency" in t]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        
        # Calculate block size statistics
        avg_block_size = sum(self.block_sizes) / len(self.block_sizes) if self.block_sizes else 0
        max_block_size = max(self.block_sizes) if self.block_sizes else 0
        
        # Calculate consensus statistics
        avg_consensus_time = sum(self.consensus_times) / len(self.consensus_times) if self.consensus_times else 0
        
        # Calculate network latency statistics
        avg_network_latency = sum(self.network_latencies) / len(self.network_latencies) if self.network_latencies else 0
        
        # Calculate shard statistics
        shard_stats = {}
        for shard_id, metrics in self.shard_metrics.items():
            shard_stats[shard_id] = {
                "blocks_mined": metrics["blocks_mined"],
                "transactions_processed": metrics["transactions_processed"],
                "avg_block_time": metrics["avg_block_time"],
                "throughput": metrics["transactions_processed"] / total_time if total_time > 0 else 0
            }
        
        return {
            "total_time": total_time,
            "total_transactions": self.total_transactions,
            "total_blocks": self.total_blocks,
            "avg_block_time": avg_block_time,
            "throughput": throughput,
            "avg_latency": avg_latency,
            "block_size_stats": {
                "average": avg_block_size,
                "maximum": max_block_size
            },
            "consensus_stats": {
                "average_time": avg_consensus_time,
                "total_consensus_events": len(self.consensus_times)
            },
            "network_stats": {
                "average_latency": avg_network_latency,
                "total_measurements": len(self.network_latencies)
            },
            "shard_stats": shard_stats
        }

def generate_key_pair():
    """Generate RSA key pair"""
    key = RSA.generate(2048)
    return key, key.publickey()

def sign_message(private_key: RSA.RsaKey, message: str) -> str:
    """Sign a message using RSA private key"""
    h = SHA256.new(message.encode())
    signature = pkcs1_15.new(private_key).sign(h)
    return signature.hex()

async def run_node(node: BlockchainNode, stop_event: asyncio.Event):
    """Run a blockchain node"""
    await node.start()
    
    # Start mining blocks
    while not stop_event.is_set():
        if node.pending_transactions:
            logger.info(f"Node {node.node_id} mining block with {len(node.pending_transactions)} transactions")
            block = await node.mine_block()
            if block:
                logger.info(f"Node {node.node_id} mined block {block.index} with hash {block.hash[:8]}")
        await asyncio.sleep(1)  # Mine every second if there are transactions
    
    await node.stop()

async def test_blockchain_voting():
    """Test the blockchain voting system"""
    try:
        # Initialize components
        web3 = Web3()
        shard_manager = ShardManager(num_shards=5)  # 5 shards
        nodes = []
        election_manager = ElectionManager()
        
        # Start performance metrics
        metrics = PerformanceMetrics()
        metrics.start_test()
        
        # Create nodes for each shard (2 nodes per shard)
        for node_num in range(10):  # 10 nodes total (2 per shard)
            node_id = f"node_{node_num}"
            port = 5000 + node_num
            # Get shard assignment from shard manager
            shard_id = shard_manager.assign_node_to_shard(node_id)
            node = BlockchainNode(node_id, shard_id, web3, "127.0.0.1", port)
            nodes.append(node)
        
        # Start all nodes
        start_tasks = [node.start() for node in nodes]
        await asyncio.gather(*start_tasks)
        logger.info("All nodes started successfully")
        
        # Discover peers and measure network latency
        discover_tasks = []
        for node in nodes:
            start_time = time.time()
            discover_tasks.append(node.discover_peers())
            latency = time.time() - start_time
            metrics.record_network_latency(latency)
        
        await asyncio.gather(*discover_tasks)
        logger.info("Peer discovery completed")
        
        # Initialize random seed for reproducibility
        random.seed(123)
        
        # Registration Phase
        logger.info("\n=== REGISTRATION PHASE ===")
        voters = {}
        for i in range(1, 31):  # Create 30 unique voters
            voter_id = f"voter{i}"
            if election_manager.register_voter(voter_id):
                voters[voter_id] = {}
                logger.info(f"Registered voter: {voter_id}")
            else:
                logger.warning(f"Failed to register voter: {voter_id}")
        
        # Wait for registration phase to complete
        while not election_manager.should_transition_phase():
            await asyncio.sleep(1)
        
        # Transition to voting phase
        current_phase = election_manager.transition_to_next_phase()
        logger.info(f"\n=== {current_phase.name} PHASE ===")
        
        # Voting Phase
        num_rounds = 3  # We'll send 3 rounds of votes
        for round_num in range(num_rounds):
            logger.info(f"\nSending round {round_num + 1} of votes...")
            
            # Calculate start and end voter indices for this round
            start_voter = round_num * 10 + 1
            end_voter = start_voter + 10
            
            # Get voters for this round
            round_voters = [f"voter{i}" for i in range(start_voter, end_voter)]
            
            for voter_id in round_voters:
                if election_manager.can_cast_vote(voter_id):
                    # Randomly assign a candidate
                    candidate_num = random.randint(1, 3)
                    vote = {
                        "type": "vote",
                        "voter_id": voter_id,
                        "candidate_id": f"candidate{candidate_num}"
                    }
                    
                    # Get shard assignment for this vote
                    vote_shard = shard_manager.get_shard_for_vote(vote["voter_id"])
                    vote["shard_id"] = vote_shard
                    
                    # Record transaction submission time
                    metrics.record_transaction_submission(voter_id)
                    
                    # Send vote to appropriate shard nodes
                    for node in nodes:
                        if node.shard_id == vote_shard:
                            await node.add_transaction(vote)
                    
                    # Record the vote
                    election_manager.record_vote(voter_id)
                    logger.info(f"Vote cast by {voter_id} for {vote['candidate_id']}")
                else:
                    logger.warning(f"Cannot cast vote for {voter_id} - not registered or already voted")
            
            # Mine blocks after each round
            logger.info(f"\nMining blocks for round {round_num + 1}...")
            for node in nodes:
                if node.pending_transactions:
                    # Record consensus start time
                    consensus_start = time.time()
                    
                    block = await node.mine_block()
                    if block:
                        # Record consensus time
                        consensus_time = time.time() - consensus_start
                        metrics.record_consensus_time(consensus_time)
                        
                        metrics.record_block_mined(block.hash, block.index, node.shard_id, len(block.transactions))
                        # Record transaction inclusion times
                        for tx in block.transactions:
                            if tx.get("type") == "vote":
                                metrics.record_transaction_inclusion(tx["voter_id"], block.index, node.shard_id)
                        logger.info(f"Mined block {block.index} with hash {block.hash[:8]}")
            
            # Wait a bit between rounds to ensure proper synchronization
            await asyncio.sleep(1)
        
        # Wait for voting phase to complete
        while not election_manager.should_transition_phase():
            await asyncio.sleep(1)
        
        # Transition to tallying phase
        current_phase = election_manager.transition_to_next_phase()
        logger.info(f"\n=== {current_phase.name} PHASE ===")
        
        # Tallying Phase
        logger.info("\nCalculating final vote tally...")
        vote_tally = {"candidate1": 0, "candidate2": 0, "candidate3": 0}
        
        # Only count votes from one node per shard to avoid double-counting
        counted_shards = set()
        for node in nodes:
            if node.shard_id in counted_shards:
                continue
            counted_shards.add(node.shard_id)
            
            # Count votes from this node's chain
            for block in node.chain:
                for vote in block.transactions:
                    if vote.get("type") == "vote":
                        candidate = vote.get("candidate_id")
                        if candidate in vote_tally:
                            vote_tally[candidate] += 1
                            logger.info(f"Counted vote from {vote['voter_id']} for {candidate}")
        
        # Wait for tallying phase to complete
        while not election_manager.should_transition_phase():
            await asyncio.sleep(1)
        
        # Transition to results phase
        current_phase = election_manager.transition_to_next_phase()
        logger.info(f"\n=== {current_phase.name} PHASE ===")
        
        # Results Phase
        logger.info("\nFinal Vote Tally:")
        for candidate, votes in vote_tally.items():
            logger.info(f"{candidate}: {votes} votes")
        
        # Determine winner
        winner = max(vote_tally.items(), key=lambda x: x[1])
        logger.info(f"\nWinner: {winner[0]} with {winner[1]} votes")
        
        # Check if there's a tie
        if len(set(vote_tally.values())) == 1:
            logger.info("Note: There is a tie!")
        
        # Print performance metrics
        metrics_data = metrics.get_metrics()
        logger.info("\nPerformance Metrics:")
        if isinstance(metrics_data, str):
            logger.info(metrics_data)
        else:
            logger.info(f"Total Test Duration: {metrics_data['total_time']:.2f} seconds")
            logger.info(f"Total Transactions: {metrics_data['total_transactions']}")
            logger.info(f"Total Blocks: {metrics_data['total_blocks']}")
            logger.info(f"Average Block Time: {metrics_data['avg_block_time']:.2f} seconds")
            logger.info(f"Transaction Throughput: {metrics_data['throughput']:.2f} tx/s")
            logger.info(f"Average Transaction Latency: {metrics_data['avg_latency']:.2f} seconds")
            
            # Print block size statistics
            logger.info("\nBlock Size Statistics:")
            logger.info(f"Average Block Size: {metrics_data['block_size_stats']['average']:.2f} transactions")
            logger.info(f"Maximum Block Size: {metrics_data['block_size_stats']['maximum']} transactions")
            
            # Print consensus statistics
            logger.info("\nConsensus Statistics:")
            logger.info(f"Average Consensus Time: {metrics_data['consensus_stats']['average_time']:.2f} seconds")
            logger.info(f"Total Consensus Events: {metrics_data['consensus_stats']['total_consensus_events']}")
            
            # Print network statistics
            logger.info("\nNetwork Statistics:")
            logger.info(f"Average Network Latency: {metrics_data['network_stats']['average_latency']:.2f} seconds")
            logger.info(f"Total Network Measurements: {metrics_data['network_stats']['total_measurements']}")
            
            # Print shard statistics
            logger.info("\nShard Statistics:")
            for shard_id, stats in metrics_data['shard_stats'].items():
                logger.info(f"\nShard {shard_id}:")
                logger.info(f"  Blocks Mined: {stats['blocks_mined']}")
                logger.info(f"  Transactions Processed: {stats['transactions_processed']}")
                logger.info(f"  Average Block Time: {stats['avg_block_time']:.2f} seconds")
                logger.info(f"  Throughput: {stats['throughput']:.2f} tx/s")

        # Print detailed blockchain state for each node
        logger.info("\n=== NODE BLOCKCHAIN STATES ===")
        for node in nodes:
            logger.info(f"\nNode {node.node_id} (Shard {node.shard_id}):")
            logger.info(f"  Chain size: {len(node.chain)} blocks")
            # Validate chain
            is_valid = node.is_chain_valid()
            logger.info(f"  Chain valid: {is_valid}")
            for block in node.chain:
                logger.info(f"    Block {block.index} | Hash: {block.hash[:8]} | Prev: {block.previous_hash[:8]} | Txns: {len(block.transactions)}")
                for tx in block.transactions:
                    if tx.get("type") == "vote":
                        logger.info(f"      Vote: voter_id={tx['voter_id']}, candidate_id={tx['candidate_id']}, shard_id={tx.get('shard_id')}")
                    else:
                        logger.info(f"      Tx: {tx}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
    finally:
        # Stop all nodes
        stop_tasks = [node.stop() for node in nodes]
        await asyncio.gather(*stop_tasks)
        logger.info("All nodes stopped")

if __name__ == "__main__":
    try:
        asyncio.run(test_blockchain_voting())
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise 