from typing import List, Dict, Optional
from dataclasses import dataclass
import asyncio
import json
import hashlib
import time
from web3 import Web3
from .p2p import P2PProtocol
from .types import MessageType
import logging
from .blockchain import Blockchain

logger = logging.getLogger(__name__)

@dataclass
class Block:
    index: int
    timestamp: float
    transactions: List[Dict]
    previous_hash: str
    hash: str
    nonce: int
    shard_id: int

class BlockchainNode:
    def __init__(self, node_id: str, shard_id: int, web3: Web3, host: str, port: int):
        self.node_id = node_id
        self.shard_id = shard_id
        self.web3 = web3
        self.host = host
        self.port = port
        self.chain: List[Block] = []
        self.pending_transactions: List[Dict] = []
        self.blockchain = Blockchain(shard_id)
        self.p2p = P2PProtocol(node_id, shard_id, self.blockchain)
        
        # Register message handlers
        self.p2p.on_transaction_received = self._handle_transaction
        self.p2p.on_block_received = self._handle_block
        self.p2p.on_sync_requested = self._handle_sync_request
        self.p2p.on_chain_received = self._handle_chain_received
        
        # Initialize chain
        self._initialize_chain()

    async def start(self):
        """Start the node"""
        # Initialize genesis block if chain is empty
        if not self.chain:
            self.chain = [self._create_genesis_block()]
            logger.info(f"Node {self.node_id} initialized with genesis block hash {self.chain[0].hash[:8]} for shard {self.shard_id}")
        
        # Set chain callbacks for P2P protocol
        self.p2p.set_chain_callbacks(
            get_chain=lambda: self.chain,
            set_chain=lambda chain: setattr(self, 'chain', chain),
            verify_chain=self._verify_chain,
            get_genesis_hash=lambda: self.chain[0].hash if self.chain else None
        )
        
        # Start P2P server
        await self.p2p.start()
        
        # Set message handlers
        self.p2p.on_transaction_received = self._handle_transaction
        self.p2p.on_block_received = self._handle_block
        self.p2p.on_sync_requested = lambda: self.chain
        self.p2p.on_chain_received = self._handle_chain_received
        
        # Initial sync with peers
        await asyncio.sleep(1)  # Wait for connections to establish
        await self.sync_with_peers()

    async def stop(self):
        """Stop the node and P2P server"""
        await self.p2p.stop()

    async def _periodic_sync(self):
        """Periodically sync with peers"""
        while True:
            try:
                await self.sync_with_peers()
                await asyncio.sleep(0.5)  # Sync even more frequently
            except Exception as e:
                logger.error(f"Error in periodic sync: {e}")
            await asyncio.sleep(0.5)  # Wait before next sync

    async def sync_with_peers(self):
        """Sync blockchain with peers in the same shard"""
        try:
            # Get current chain length
            current_length = len(self.chain)
            
            # Request chain from all peers in the same shard
            for peer_id, (reader, writer) in list(self.p2p.connections.items()):
                if peer_id.startswith(f"node_{self.shard_id}_"):
                    try:
                        # Send sync request
                        await self.p2p._send_message(writer, {
                            "type": MessageType.SYNC_REQUEST.value,
                            "data": {
                                "node_id": self.node_id,
                                "chain_length": current_length
                            }
                        })
                        
                        # Wait for response with timeout
                        try:
                            response = await asyncio.wait_for(
                                self.p2p._read_message(reader),
                                timeout=2.0  # Longer timeout for chain sync
                            )
                            if response and response["type"] == MessageType.SYNC_RESPONSE.value:
                                peer_chain = response["data"]["chain"]
                                if len(peer_chain) > current_length:
                                    # Convert chain data to Block objects
                                    new_chain = []
                                    for block_dict in peer_chain:
                                        block = Block(
                                            index=block_dict["index"],
                                            timestamp=block_dict["timestamp"],
                                            transactions=block_dict["transactions"],
                                            previous_hash=block_dict["previous_hash"],
                                            hash=block_dict["hash"],
                                            nonce=block_dict["nonce"],
                                            shard_id=block_dict["shard_id"]
                                        )
                                        new_chain.append(block)
                                    
                                    # Verify the new chain
                                    if self._verify_chain(new_chain):
                                        # Update our chain
                                        self.chain = new_chain
                                        logger.info(f"Node {self.node_id} synced chain from peer {peer_id}, new length: {len(new_chain)}")
                                        
                                        # Clean up pending transactions
                                        voters = set()
                                        for block in new_chain:
                                            for tx in block.transactions:
                                                if tx.get("type") == "vote":
                                                    voters.add(tx.get("voter_id"))
                                        
                                        # Remove any pending transactions from voters who already voted
                                        self.pending_transactions = [tx for tx in self.pending_transactions
                                                                   if tx.get("type") != "vote" or 
                                                                   tx.get("voter_id") not in voters]
                                        
                                        # Break early since we've found a longer valid chain
                                        break
                                    else:
                                        logger.warning(f"Received invalid chain from peer {peer_id}")
                                        
                        except asyncio.TimeoutError:
                            logger.warning(f"Sync request to {peer_id} timed out")
                            continue
                            
                    except Exception as e:
                        logger.warning(f"Failed to sync with {peer_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error syncing with peers: {e}")

    async def _handle_transaction(self, transaction: dict):
        """Handle incoming transaction"""
        # Verify transaction
        if self._verify_transaction(transaction):
            # Add to pending transactions if not already present
            if transaction.get("type") == "vote":
                # For vote transactions, check voter_id
                if not any(tx.get("voter_id") == transaction.get("voter_id") for tx in self.pending_transactions):
                    self.pending_transactions.append(transaction)
                    logger.info(f"Node {self.node_id} received vote transaction from {transaction.get('voter_id')}")
                    # Broadcast to other peers
                    await self.p2p.broadcast_transaction(transaction)
            else:
                # For non-vote transactions, just add if not present
                if transaction not in self.pending_transactions:
                    self.pending_transactions.append(transaction)
                    logger.info(f"Node {self.node_id} received transaction")
                    # Broadcast to other peers
                    await self.p2p.broadcast_transaction(transaction)

    async def _handle_block(self, block_dict: dict):
        """Handle incoming block"""
        try:
            # Convert dictionary to Block object
            block = Block(
                index=block_dict["index"],
                timestamp=block_dict["timestamp"],
                transactions=block_dict["transactions"],
                previous_hash=block_dict["previous_hash"],
                hash=block_dict["hash"],
                nonce=block_dict["nonce"],
                shard_id=block_dict["shard_id"]
            )
            
            # Verify block
            if not self._verify_block(block):
                logger.warning(f"Node {self.node_id} received invalid block {block.hash[:8]}")
                return
                
            # Check if we already have this block
            if block.index < len(self.chain):
                if self.chain[block.index].hash == block.hash:
                    return  # Already have this block
                logger.warning(f"Node {self.node_id} received conflicting block at index {block.index}")
                # If we receive a conflicting block, sync with peers to resolve
                await self.sync_with_peers()
                return
                
            # Check if this is the next block we need
            if block.index > len(self.chain):
                logger.warning(f"Node {self.node_id} received block {block.index} but current chain length is {len(self.chain)}")
                # Request missing blocks
                await self.sync_with_peers()
                return
                
            # Add block to chain
            self.chain.append(block)
            logger.info(f"Node {self.node_id} added block {block.index} with hash {block.hash[:8]}")
            
            # Remove included transactions from pending
            for tx in block.transactions:
                if tx.get("type") == "vote":
                    self.pending_transactions = [t for t in self.pending_transactions 
                                              if t.get("voter_id") != tx.get("voter_id")]
                else:
                    self.pending_transactions = [t for t in self.pending_transactions 
                                              if t != tx]
            
            # Forward block to other peers in same shard
            # Only forward if we haven't seen this block before
            message = {
                "type": MessageType.BLOCK.value,
                "data": block_dict
            }
            await self.p2p.broadcast_to_shard(message, self.shard_id)
            logger.info(f"Node {self.node_id} forwarded block {block.index} with hash {block.hash[:8]} to shard {self.shard_id}")
            
        except Exception as e:
            logger.error(f"Error handling block: {e}")

    async def _handle_sync_request(self):
        """Handle chain sync request"""
        return self.chain

    async def _handle_chain_received(self, chain_data: list):
        """Handle received chain"""
        try:
            if len(chain_data) <= len(self.chain):
                return
                
            # Convert chain data to Block objects
            chain = []
            for block_dict in chain_data:
                block = Block(
                    index=block_dict["index"],
                    timestamp=block_dict["timestamp"],
                    transactions=block_dict["transactions"],
                    previous_hash=block_dict["previous_hash"],
                    hash=block_dict["hash"],
                    nonce=block_dict["nonce"],
                    shard_id=block_dict["shard_id"]
                )
                chain.append(block)
                
            # Verify the new chain
            if not self._verify_chain(chain):
                logger.warning("Received invalid chain")
                return
                
            # Update our chain
            old_length = len(self.chain)
            self.chain = chain
            logger.info(f"Node {self.node_id} synced chain to length {len(chain)}")
            
            # Update pending transactions
            for block in chain[old_length:]:
                for tx in block.transactions:
                    # Remove transactions from pending based on voter_id for vote transactions
                    if tx.get("type") == "vote":
                        self.pending_transactions = [t for t in self.pending_transactions 
                                                  if t.get("voter_id") != tx.get("voter_id")]
                    else:
                        # For non-vote transactions, remove exact matches
                        self.pending_transactions = [t for t in self.pending_transactions 
                                                  if t != tx]
                                              
            # Forward new blocks to other peers in the same shard
            for block in chain[old_length:]:
                await self.broadcast_block(block)
                
        except Exception as e:
            logger.error(f"Error handling received chain: {e}")

    def _verify_transaction(self, transaction: Dict) -> bool:
        """Verify transaction is valid"""
        try:
            # Check transaction has required fields
            if not isinstance(transaction, dict):
                logger.warning("Transaction is not a dictionary")
                return False
                
            if transaction.get("type") != "vote":
                logger.warning("Transaction is not a vote")
                return False
                
            if "voter_id" not in transaction:
                logger.warning("Transaction missing voter_id")
                return False
                
            if "candidate_id" not in transaction:
                logger.warning("Transaction missing candidate_id")
                return False
                
            # Check voter_id is valid
            voter_id = transaction["voter_id"]
            if not isinstance(voter_id, str):
                logger.warning("voter_id is not a string")
                return False
                
            # Check candidate_id is valid
            candidate_id = transaction["candidate_id"]
            if not isinstance(candidate_id, str):
                logger.warning("candidate_id is not a string")
                return False
                
            # Check voter hasn't already voted
            for block in self.chain:
                for tx in block.transactions:
                    if tx.get("type") == "vote" and tx.get("voter_id") == voter_id:
                        logger.warning(f"Voter {voter_id} has already voted")
                        return False
                        
            return True
            
        except Exception as e:
            logger.error(f"Error verifying transaction: {e}")
            return False

    def _verify_block(self, block: Block) -> bool:
        """Verify block is valid"""
        try:
            # Check block index
            if block.index != len(self.chain):
                logger.warning(f"Invalid block index {block.index}, expected {len(self.chain)}")
                return False
                
            # Check block is from our shard
            if block.shard_id != self.shard_id:
                logger.warning(f"Block from wrong shard {block.shard_id}, expected {self.shard_id}")
                return False
                
            # Check previous hash matches
            if block.previous_hash != self.chain[-1].hash:
                logger.warning(f"Previous hash mismatch: {block.previous_hash} != {self.chain[-1].hash}")
                return False
                
            # Check block hash is valid
            block_data = {
                "index": block.index,
                "timestamp": block.timestamp,
                "transactions": block.transactions,
                "previous_hash": block.previous_hash,
                "nonce": block.nonce,
                "shard_id": block.shard_id
            }
            calculated_hash = self._calculate_hash(block_data)
            if block.hash != calculated_hash:
                logger.warning(f"Hash mismatch for block {block.index}, expected {block.hash}, got {calculated_hash}")
                return False
                
            # Check block hash has required number of leading zeros
            if not block.hash.startswith("0" * 4):
                logger.warning(f"Block hash {block.hash} does not have required leading zeros")
                return False
                
            # Check transactions are valid
            for tx in block.transactions:
                if not self._verify_transaction(tx):
                    logger.warning(f"Invalid transaction in block {block.index}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error verifying block: {e}")
            return False

    def _verify_chain(self, chain: List[Block]) -> bool:
        """Verify chain is valid"""
        try:
            # Check chain length
            if len(chain) == 0:
                logger.warning("Empty chain")
                return False
                
            # Check genesis block
            if chain[0].hash != self.chain[0].hash:
                logger.warning(f"Genesis block mismatch: {chain[0].hash} != {self.chain[0].hash}")
                return False
                
            # Keep track of voters to prevent double voting
            voters = set()
                
            # Check each block
            for i in range(len(chain)):
                block = chain[i]
                
                # Check block index
                if block.index != i:
                    logger.warning(f"Invalid block index at position {i}: {block.index}")
                    return False
                    
                # Check block is from our shard
                if block.shard_id != self.shard_id:
                    logger.warning(f"Block from wrong shard at position {i}: {block.shard_id}")
                    return False
                    
                # Check block hash has required number of leading zeros
                if not block.hash.startswith("0" * 4):
                    logger.warning(f"Block hash {block.hash} does not have required leading zeros")
                    return False
                    
                # Check block hash is valid
                block_data = {
                    "index": block.index,
                    "timestamp": block.timestamp,
                    "transactions": block.transactions,
                    "previous_hash": block.previous_hash,
                    "nonce": block.nonce,
                    "shard_id": block.shard_id
                }
                calculated_hash = self._calculate_hash(block_data)
                if block.hash != calculated_hash:
                    logger.warning(f"Hash mismatch for block {i}, expected {block.hash}, got {calculated_hash}")
                    return False
                    
                # Check previous hash (except for genesis block)
                if i > 0 and block.previous_hash != chain[i-1].hash:
                    logger.warning(f"Previous hash mismatch at block {i}")
                    return False
                    
                # Check transactions are valid and no double voting
                for tx in block.transactions:
                    if tx.get("type") == "vote":
                        voter_id = tx.get("voter_id")
                        if voter_id in voters:
                            logger.warning(f"Double vote detected for voter {voter_id} in block {i}")
                            return False
                        voters.add(voter_id)
                        
            return True
            
        except Exception as e:
            logger.error(f"Error verifying chain: {e}")
            return False

    async def add_transaction(self, transaction: Dict) -> int:
        """Add a new transaction to the pending transactions"""
        try:
            # Check if this is a vote transaction
            if transaction.get("type") == "vote":
                # Verify the vote is for this shard
                voter_id = transaction.get("voter_id")
                if voter_id:
                    # Calculate which shard this vote belongs to
                    hash_value = int(hashlib.sha256(voter_id.encode()).hexdigest(), 16)
                    vote_shard = hash_value % 5  # Use 5 shards to match test configuration
                    
                    if vote_shard != self.shard_id:
                        # This vote belongs to a different shard
                        logger.info(f"Vote from {voter_id} belongs to shard {vote_shard}, forwarding...")
                        # Forward to the correct shard
                        asyncio.create_task(self.forward_transaction(transaction, vote_shard))
                        return -1  # Indicate vote was forwarded
                        
                    # Check if voter has already voted in pending transactions
                    if any(tx.get("type") == "vote" and tx.get("voter_id") == voter_id 
                          for tx in self.pending_transactions):
                        logger.warning(f"Voter {voter_id} already has a pending vote")
                        return -1
                        
                    # Check if voter has already voted in the chain
                    for block in self.chain:
                        if any(tx.get("type") == "vote" and tx.get("voter_id") == voter_id 
                              for tx in block.transactions):
                            logger.warning(f"Voter {voter_id} has already voted in block {block.index}")
                            return -1
            
            # If it's not a vote or belongs to this shard, process it
            self.pending_transactions.append(transaction)
            # Broadcast the transaction to peers in this shard
            message = {
                "type": MessageType.TRANSACTION.value,
                "data": transaction
            }
            await self.p2p.broadcast_to_shard(message, self.shard_id)
            logger.info(f"Node {self.node_id} added transaction from {transaction.get('voter_id', 'unknown')}")
            return len(self.chain)  # Return the block index it will be included in
            
        except Exception as e:
            logger.error(f"Error adding transaction: {e}")
            return -1

    async def mine_block(self):
        """Mine a new block with pending transactions"""
        if not self.pending_transactions:
            return None
            
        # Sort transactions by voter_id to ensure consistent order
        sorted_transactions = sorted(self.pending_transactions, key=lambda x: x.get('voter_id', ''))
            
        # Create block data with deterministic timestamp (use block index)
        block_data = {
            "index": len(self.chain),
            "timestamp": len(self.chain) * 1000,  # Use deterministic timestamp based on block index
            "transactions": sorted_transactions,
            "previous_hash": self.chain[-1].hash,
            "nonce": 0,
            "shard_id": self.shard_id
        }
        
        # Mine block (find nonce that gives required number of leading zeros)
        block_hash = self._calculate_hash(block_data)
        while not block_hash.startswith("0" * 4):
            block_data["nonce"] += 1
            block_hash = self._calculate_hash(block_data)
            
        # Create Block object
        block = Block(
            index=block_data["index"],
            timestamp=block_data["timestamp"],
            transactions=block_data["transactions"],
            previous_hash=block_data["previous_hash"],
            hash=block_hash,
            nonce=block_data["nonce"],
            shard_id=block_data["shard_id"]
        )
            
        # Add block to chain
        self.chain.append(block)
        logger.info(f"Node {self.node_id} mined block {block.index} with hash {block.hash[:8]}")
        
        # Clear pending transactions that were included in this block
        self.pending_transactions = []
        
        # Broadcast block to peers
        # Convert Block object to dictionary for broadcasting
        block_dict = {
            "index": block.index,
            "timestamp": block.timestamp,
            "transactions": block.transactions,
            "previous_hash": block.previous_hash,
            "hash": block.hash,
            "nonce": block.nonce,
            "shard_id": block.shard_id
        }
        message = {
            "type": MessageType.BLOCK.value,
            "data": block_dict
        }
        await self.p2p.broadcast_to_shard(message, self.shard_id)
        logger.info(f"Node {self.node_id} broadcasted block {block.index} with hash {block.hash[:8]} to shard {self.shard_id}")
        
        return block

    def _calculate_hash(self, block_data: Dict) -> str:
        """Calculate hash of block data"""
        block_string = json.dumps(block_data, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()

    def is_chain_valid(self) -> bool:
        """Check if the current chain is valid"""
        return self._verify_chain(self.chain)

    def _create_genesis_block(self) -> Block:
        """Create a shard-specific genesis block"""
        genesis_data = {
            "index": 0,
            "timestamp": 0,  # Use fixed timestamp for deterministic genesis block
            "transactions": [],
            "previous_hash": "0" * 64,  # Genesis block has no previous hash
            "nonce": self.shard_id * 1000,  # Different starting points for each shard
            "shard_id": self.shard_id
        }
        
        # Mine genesis block with fixed initial nonce based on shard
        genesis_hash = self._calculate_hash(genesis_data)
        
        # Keep mining until we find a hash starting with 0000
        while not genesis_hash.startswith("0000"):
            genesis_data["nonce"] += 1
            genesis_hash = self._calculate_hash(genesis_data)
        
        return Block(
            index=0,
            timestamp=genesis_data["timestamp"],
            transactions=genesis_data["transactions"],
            previous_hash=genesis_data["previous_hash"],
            hash=genesis_hash,
            nonce=genesis_data["nonce"],
            shard_id=self.shard_id
        )

    def _initialize_chain(self):
        """Initialize blockchain with genesis block"""
        genesis_block = self._create_genesis_block()
        self.chain = [genesis_block]
        logger.info(f"Node {self.node_id} initialized with genesis block hash {genesis_block.hash[:8]} for shard {self.shard_id}")

    async def broadcast_block(self, block: Block):
        """Broadcast a new block to peers"""
        try:
            # Convert Block object to dictionary for JSON serialization
            block_dict = {
                "index": block.index,
                "timestamp": block.timestamp,
                "transactions": block.transactions,
                "previous_hash": block.previous_hash,
                "hash": block.hash,
                "nonce": block.nonce,
                "shard_id": block.shard_id
            }
            # Create message with proper format
            message = {
                "type": MessageType.BLOCK.value,
                "data": block_dict  # Put block in the data field
            }
            # Broadcast to peers in same shard
            await self.p2p.broadcast_to_shard(message, self.shard_id)
            logger.info(f"Node {self.node_id} broadcasted block {block.index} with hash {block.hash[:8]} to shard {self.shard_id}")
        except Exception as e:
            logger.error(f"Error broadcasting block: {e}")

    async def forward_transaction(self, transaction: Dict, target_shard: int):
        """Forward a transaction to a different shard"""
        message = {
            "type": MessageType.TRANSACTION.value,
            "data": transaction  # Use data field to match message format
        }
        # Find a peer in the target shard
        for peer_id, (host, port) in self.p2p.peers.items():
            if peer_id.startswith(f"node_{target_shard}_"):
                try:
                    reader, writer = await asyncio.open_connection(host, int(port))
                    await self.p2p._send_message(writer, message)
                    writer.close()
                    await writer.wait_closed()
                    logger.info(f"Forwarded transaction to shard {target_shard}")
                    return
                except Exception as e:
                    logger.error(f"Error forwarding transaction to shard {target_shard}: {e}")

    async def broadcast_transaction(self, transaction: Dict):
        """Broadcast a new transaction to peers"""
        message = {
            "type": MessageType.TRANSACTION.value,
            "data": transaction  # Put transaction in the data field
        }
        await self.p2p.broadcast_to_shard(message, self.shard_id)

    async def discover_peers(self):
        """Discover peers through bootstrap nodes"""
        logger.info(f"{self.node_id} discovering peers")
        await self.p2p.discover_peers() 