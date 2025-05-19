from typing import Dict, List, Optional, Set, Tuple
import asyncio
import aiohttp
import json
import time
import logging
import socket
from dataclasses import dataclass
from .types import MessageType, Block, Transaction
from .consensus import ConsensusManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Peer:
    node_id: str
    shard_id: int
    host: str
    port: int
    last_seen: float = 0.0
    connection: Optional[asyncio.StreamWriter] = None
    retry_count: int = 0
    max_retries: int = 3
    retry_delay: float = 1.0

class P2PProtocol:
    def __init__(self, node_id: str, shard_id: int, blockchain):
        self.node_id = node_id
        self.blockchain = blockchain
        self.shard_id = shard_id
        self.host = "127.0.0.1"
        self.port = 5000 + (shard_id * 2)  # Each shard uses 2 ports
        self.consensus_manager = ConsensusManager(shard_id)
        self.consensus_manager.register_miner(node_id)  # Register this node as a miner
        self.connections = {}  # node_id -> connection
        self.tasks = []
        self.logger = logging.getLogger(f"s3voting.blockchain.p2p.shard_{shard_id}")
        self.server = None
        self._consensus_task = None

    async def start(self):
        """Start the P2P server and consensus process"""
        try:
            # Create socket with reuse options
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            
            # Try to bind with retries
            max_retries = 3
            retry_delay = 1.0
            
            for attempt in range(max_retries):
                try:
                    sock.bind((self.host, self.port))
                    break
                except OSError as e:
                    if attempt == max_retries - 1:
                        raise
                    self.logger.warning(f"Port {self.port} in use, retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
            
            sock.listen(5)
            self.server = await asyncio.start_server(
                self._handle_connection,
                sock=sock
            )
            
            self.logger.info(f"P2P server started on {self.host}:{self.port}")
            
            # Start consensus task
            self._consensus_task = asyncio.create_task(self._run_consensus())
            self.tasks.append(self._consensus_task)
            
            # Start peer discovery
            asyncio.create_task(self.discover_peers())
            
        except Exception as e:
            self.logger.error(f"Error starting P2P server: {e}")
            raise

    async def stop(self):
        """Stop the P2P server and clean up connections"""
        try:
            # Cancel consensus task
            if self._consensus_task:
                self._consensus_task.cancel()
                try:
                    await self._consensus_task
                except asyncio.CancelledError:
                    pass

            # Cancel all background tasks
            for task in self.tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            self.tasks.clear()
            
            # Close all peer connections
            for peer_id, (reader, writer) in list(self.connections.items()):
                try:
                    if not writer.is_closing():
                        writer.close()
                        await writer.wait_closed()
                except Exception as e:
                    self.logger.warning(f"Error closing connection to {peer_id}: {e}")
            
            # Clear connection data
            self.connections.clear()
            
            # Close server
            if self.server:
                self.server.close()
                await self.server.wait_closed()
                self.logger.info("P2P server stopped")
                
        except Exception as e:
            self.logger.error(f"Error stopping P2P server: {e}")
            raise

    async def _run_consensus(self):
        """Run the consensus process"""
        while True:
            try:
                # Select leaders for current slot
                m_leader, b_leader = self.consensus_manager.select_leaders()
                if not m_leader or not b_leader:
                    await asyncio.sleep(1)
                    continue

                # If we're the main leader, propose a block
                if m_leader.node_id == self.node_id:
                    block = await self.blockchain.create_block()
                    if block:
                        raw_block = {
                            "hash": block.hash,
                            "previous_hash": block.previous_hash,
                            "timestamp": block.timestamp,
                            "transactions": [tx.to_dict() for tx in block.transactions],
                            "shard_id": self.shard_id
                        }
                        if await self.consensus_manager.propose_block(self.node_id, raw_block):
                            await self.broadcast_block(block)

                # Wait for consensus timeout
                await asyncio.sleep(self.consensus_manager.state.consensus_timeout)

                # If we're the backup leader and no block was finalized, take over
                if b_leader.node_id == self.node_id:
                    await self.consensus_manager.handle_leader_failure(m_leader.node_id, "")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consensus process: {e}")
                await asyncio.sleep(1)

    async def handle_message(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, message: Dict):
        """Handle incoming P2P messages"""
        msg_type = message.get("type")
        if msg_type == "block":
            await self._handle_block_message(message)
        elif msg_type == "signature":
            await self._handle_signature_message(message)
        elif msg_type == "sync_request":
            await self._handle_sync_request(reader, writer, message)
        elif msg_type == "sync_response":
            await self._handle_sync_response(message)

    async def _handle_block_message(self, message: Dict):
        """Handle incoming block message"""
        block_data = message["data"]
        block_hash = block_data["hash"]
        
        # Sign the block if we're not the proposer
        if block_data.get("proposer") != self.node_id:
            signature = await self.blockchain.sign_block(block_hash)
            if signature:
                await self.consensus_manager.sign_block(self.node_id, block_hash, signature)
                await self.broadcast_signature(block_hash, signature)

    async def _handle_signature_message(self, message: Dict):
        """Handle incoming signature message"""
        block_hash = message["block_hash"]
        signature = message["signature"]
        node_id = message["node_id"]

        if await self.consensus_manager.sign_block(node_id, block_hash, signature):
            # If we have enough signatures, finalize the block
            if self.node_id in [l.node_id for l in self.consensus_manager.state.leaders.get(self.shard_id, (None, None))]:
                final_block = await self.consensus_manager.finalize_block(self.node_id, block_hash)
                if final_block:
                    await self.blockchain.add_block(Block.from_dict(final_block))

    async def broadcast_signature(self, block_hash: str, signature: Dict):
        """Broadcast a block signature to all peers"""
        message = {
            "type": "signature",
            "block_hash": block_hash,
            "signature": signature,
            "node_id": self.node_id
        }
        await self.broadcast_message(message)

    def set_chain_callbacks(self, get_chain, set_chain, verify_chain, get_genesis_hash):
        """Set callbacks for chain operations"""
        self.get_chain = get_chain
        self.set_chain = set_chain
        self.verify_chain = verify_chain
        self.get_genesis_hash = get_genesis_hash

    async def _periodic_chain_sync(self):
        """Periodically sync chain with peers"""
        while True:
            try:
                # Create a list of sync tasks for each peer
                sync_tasks = []
                for peer_id, (reader, writer) in list(self.peers.items()):
                    # Only sync with connected peers
                    if (not writer.is_closing() and 
                        self.peer_states.get(peer_id) == "connected"):
                        sync_tasks.append(self._sync_chains_with_peer(peer_id, reader, writer))
                
                # Wait for all sync tasks to complete
                if sync_tasks:
                    results = await asyncio.gather(*sync_tasks, return_exceptions=True)
                    # Check for failed syncs
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            peer_id = list(self.peers.keys())[i]
                            logger.error(f"Sync failed for peer {peer_id}: {result}")
                            self.peer_states[peer_id] = "disconnected"
                
                # Clean up disconnected peers
                for peer_id in list(self.peers.keys()):
                    if self.peer_states.get(peer_id) == "disconnected":
                        try:
                            reader, writer = self.peers[peer_id]
                            if not writer.is_closing():
                                writer.close()
                                await writer.wait_closed()
                            del self.peers[peer_id]
                            logger.info(f"Cleaned up disconnected peer {peer_id}")
                        except Exception as e:
                            logger.error(f"Error cleaning up peer {peer_id}: {e}")
                
                await asyncio.sleep(5)  # Increased sync interval
            except Exception as e:
                logger.error(f"Error in periodic chain sync: {e}")
                await asyncio.sleep(5)  # Wait before retrying

    async def _maintain_peers(self):
        """Periodically check and maintain peer connections"""
        while True:
            try:
                # Remove disconnected peers
                disconnected = []
                for peer_id, (reader, writer) in list(self.peers.items()):
                    try:
                        if writer.is_closing():
                            disconnected.append(peer_id)
                            writer.close()
                            await writer.wait_closed()
                    except Exception as e:
                        logger.warning(f"Error checking connection for {peer_id}: {e}")
                        disconnected.append(peer_id)
                
                for peer_id in disconnected:
                    try:
                        if peer_id in self.peers:
                            del self.peers[peer_id]
                        self.peer_states[peer_id] = "disconnected"
                        logger.warning(f"Peer {peer_id} disconnected")
                    except Exception as e:
                        logger.error(f"Error cleaning up peer {peer_id}: {e}")
                
                # Try to reconnect to disconnected peers in our shard
                for peer_id, (host, port) in list(self.peers.items()):
                    if (peer_id.startswith(f"node_{self.shard_id}_") and 
                        self.peer_states.get(peer_id) == "disconnected"):
                        try:
                            # Add delay before reconnection attempt
                            await asyncio.sleep(1)
                            await self._reconnect_to_peer(peer_id, host, port)
                        except Exception as e:
                            logger.warning(f"Failed to reconnect to {peer_id}: {e}")
                
                await asyncio.sleep(2)  # Check more frequently
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in peer maintenance: {e}")
                await asyncio.sleep(5)  # Wait longer on error

    async def _reconnect_to_peer(self, peer_id: str, host: str, port: int, max_retries: int = 3):
        """Attempt to reconnect to a peer with retries"""
        for retry in range(max_retries):
            try:
                # Skip if already connected
                if peer_id in self.peers:
                    reader, writer = self.peers[peer_id]
                    if not writer.is_closing():
                        return

                # Connect with timeout
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=5.0
                )

                # Send hello message
                await self._send_message(writer, {
                    "type": MessageType.HELLO.value,
                    "data": {
                        "node_id": self.node_id,
                        "shard_id": self.shard_id,
                        "host": self.host,
                        "port": self.port,
                        "genesis_hash": self.get_genesis_hash() if self.get_genesis_hash else None
                    }
                })

                # Wait for response
                response = await asyncio.wait_for(
                    self._read_message(reader),
                    timeout=5.0
                )

                if response and response["type"] == MessageType.HELLO.value:
                    peer_shard = response["data"]["shard_id"]
                    if peer_shard == self.shard_id:
                        self.peers[peer_id] = (reader, writer)
                        self.peer_states[peer_id] = "connected"
                        logger.info(f"Successfully reconnected to peer {peer_id}")
                        
                        # Sync chains after successful reconnection
                        await self._sync_chains_with_peer(peer_id, reader, writer)
                        return
                    else:
                        writer.close()
                        await writer.wait_closed()
                        logger.warning(f"Peer {peer_id} is in different shard")
                        return

            except asyncio.TimeoutError:
                logger.warning(f"Timeout reconnecting to {peer_id} (attempt {retry + 1}/{max_retries})")
            except ConnectionResetError:
                logger.warning(f"Connection reset while reconnecting to {peer_id} (attempt {retry + 1}/{max_retries})")
            except Exception as e:
                logger.warning(f"Error reconnecting to {peer_id}: {e} (attempt {retry + 1}/{max_retries})")

            if retry < max_retries - 1:
                await asyncio.sleep(2 ** retry)  # Exponential backoff

        logger.error(f"Failed to reconnect to {peer_id} after {max_retries} attempts")

    async def _handle_connection(self, reader, writer):
        """Handle incoming connection"""
        peer_id = None
        try:
            # Read first message which should be HELLO
            message = await self._read_message(reader)
            if not message or message["type"] != MessageType.HELLO.value:
                logger.warning("First message was not HELLO, closing connection")
                writer.close()
                await writer.wait_closed()
                return
            
            # Get peer info from HELLO message
            peer_id = message["data"]["node_id"]
            peer_shard = message["data"]["shard_id"]
            peer_host = message["data"]["host"]
            peer_port = message["data"]["port"]
            
            # Only accept connections from peers in our shard
            if peer_shard != self.shard_id:
                logger.warning(f"Rejecting connection from peer {peer_id} in shard {peer_shard}")
                writer.close()
                await writer.wait_closed()
                return
            
            # Send our HELLO response with our genesis block hash
            await self._send_message(writer, {
                "type": MessageType.HELLO.value,
                "data": {
                    "node_id": self.node_id,
                    "shard_id": self.shard_id,
                    "host": self.host,
                    "port": self.port,
                    "genesis_hash": self.get_genesis_hash() if self.get_genesis_hash else None
                }
            })
            
            # Verify genesis block matches
            peer_genesis_hash = message["data"].get("genesis_hash")
            if peer_genesis_hash and self.get_genesis_hash and peer_genesis_hash != self.get_genesis_hash():
                logger.error(f"Genesis block mismatch with peer {peer_id}. Expected {self.get_genesis_hash()}, got {peer_genesis_hash}")
                writer.close()
                await writer.wait_closed()
                return
            
            # Store connection
            self.peers[peer_id] = (peer_host, peer_port)
            self.peer_states[peer_id] = "connected"
            logger.info(f"Accepted connection from peer {peer_id}")
            
            # Immediately sync chains
            await self._sync_chains_with_peer(peer_id, reader, writer)
            
            # Handle subsequent messages
            while True:
                try:
                    message = await self._read_message(reader)
                    if not message:
                        break
                    
                    # Process message
                    await self._process_message(message, writer)
                except ConnectionResetError:
                    logger.warning(f"Connection reset by peer {peer_id}")
                    break
                except Exception as e:
                    logger.error(f"Error handling message from {peer_id}: {e}")
                    break
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error handling connection from {peer_id or 'unknown peer'}: {e}")
        finally:
            # Clean up connection
            try:
                if peer_id:
                    self.peer_states[peer_id] = "disconnected"
                    if peer_id in self.peers:
                        del self.peers[peer_id]
                    logger.info(f"Connection closed for peer {peer_id}")
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                logger.error(f"Error cleaning up connection: {e}")

    async def _sync_chains_with_peer(self, peer_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Sync chains with a peer"""
        try:
            # Check if peer is still connected
            if writer.is_closing():
                logger.warning(f"Peer {peer_id} connection is closing, skipping sync")
                return

            if not all([self.get_chain, self.set_chain, self.verify_chain, self.get_genesis_hash]):
                logger.warning("Chain callbacks not set, skipping chain sync")
                return
            
            # Check if peer is in connected state
            if self.peer_states.get(peer_id) != "connected":
                logger.warning(f"Peer {peer_id} not in connected state, skipping sync")
                return

            # Send sync request with our chain length
            try:
                await self._send_message(writer, {
                    "type": MessageType.SYNC_REQUEST.value,
                    "data": {
                        "node_id": self.node_id,
                        "chain_length": len(self.get_chain())
                    }
                })
            except Exception as e:
                logger.error(f"Failed to send sync request to {peer_id}: {e}")
                self.peer_states[peer_id] = "disconnected"
                return
            
            # Wait for response with longer timeout
            try:
                response = await asyncio.wait_for(
                    self._read_message(reader),
                    timeout=5.0  # Increased timeout for chain sync
                )
                if response and response["type"] == MessageType.SYNC_RESPONSE.value:
                    peer_chain = response["data"]["chain"]
                    
                    # Verify chain starts with same genesis block
                    if not peer_chain or peer_chain[0]["hash"] != self.get_genesis_hash():
                        logger.error(f"Genesis block mismatch during sync with peer {peer_id}")
                        return
                        
                    # If peer has longer valid chain, adopt it
                    if len(peer_chain) > len(self.get_chain()) and self.verify_chain(peer_chain):
                        self.set_chain(peer_chain)
                        logger.info(f"Synchronized chain with peer {peer_id}, new length: {len(peer_chain)}")
                        
                        # Forward new blocks to other peers
                        for block in peer_chain[len(self.get_chain()):]:
                            await self.broadcast_block(block)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout waiting for sync response from {peer_id}")
                # Mark peer as potentially disconnected
                self.peer_states[peer_id] = "disconnected"
                return
                    
        except Exception as e:
            logger.error(f"Error syncing chains with peer {peer_id}: {e}")
            self.peer_states[peer_id] = "disconnected"

    async def _process_message(self, message: dict, writer):
        """Process incoming message"""
        try:
            if not isinstance(message, dict):
                logger.error(f"Invalid message format: {message}")
                return
                
            if "type" not in message or "data" not in message:
                logger.error(f"Message missing required fields: {message}")
                return
                
            message_type = message["type"]
            message_data = message["data"]
            
            if message_type == MessageType.TRANSACTION.value:
                if self.on_transaction_received:
                    if not isinstance(message_data, dict):
                        logger.error(f"Invalid transaction data format: {message_data}")
                        return
                    await self.on_transaction_received(message_data)
                    
            elif message_type == MessageType.BLOCK.value:
                if self.on_block_received:
                    if not isinstance(message_data, dict):
                        logger.error(f"Invalid block data format: {message_data}")
                        return
                    await self.on_block_received(message_data)
                    
            elif message_type == MessageType.SYNC_REQUEST.value:
                if self.get_chain:
                    # Get chain and serialize blocks
                    chain = self.get_chain()
                    serialized_chain = [self._serialize_block(block) for block in chain]
                    
                    # Send our chain
                    await self._send_message(writer, {
                        "type": MessageType.SYNC_RESPONSE.value,
                        "data": {
                            "chain": serialized_chain
                        }
                    })
                    
            elif message_type == MessageType.SYNC_RESPONSE.value:
                if self.get_chain and self.set_chain and self.verify_chain:
                    if not isinstance(message_data, dict) or "chain" not in message_data:
                        logger.error(f"Invalid sync response data: {message_data}")
                        return
                    peer_chain = message_data["chain"]
                    # Verify chain starts with same genesis block
                    if not peer_chain or peer_chain[0]["hash"] != self.get_genesis_hash():
                        logger.error("Genesis block mismatch during sync")
                        return
                        
                    # If peer has longer valid chain, adopt it
                    if len(peer_chain) > len(self.get_chain()) and self.verify_chain(peer_chain):
                        self.set_chain(peer_chain)
                        logger.info(f"Synchronized chain, new length: {len(peer_chain)}")
                    
        except KeyError as e:
            logger.error(f"Missing key in message: {e}")
        except TypeError as e:
            logger.error(f"Invalid data type in message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def _read_message(self, reader) -> Optional[dict]:
        """Read a message from the connection"""
        try:
            # Use a lock to prevent concurrent reads
            if not hasattr(self, '_read_locks'):
                self._read_locks = {}
            
            # Get or create lock for this reader
            if reader not in self._read_locks:
                self._read_locks[reader] = asyncio.Lock()
            
            async with self._read_locks[reader]:
                # Read message length (4 bytes)
                length_bytes = await reader.readexactly(4)
                if not length_bytes:
                    return None
                    
                message_length = int.from_bytes(length_bytes, 'big')
                if message_length <= 0:
                    return None
                    
                # Read message data
                data = await reader.readexactly(message_length)
                if not data:
                    return None
                    
                # Parse JSON
                return json.loads(data.decode())
            
        except asyncio.IncompleteReadError:
            # Connection closed
            return None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error reading message: {e}")
            return None

    def _serialize_block(self, block) -> dict:
        """Convert Block object to serializable dictionary"""
        if hasattr(block, '__dict__'):
            # If block is an object, convert to dict
            return {
                'index': block.index,
                'timestamp': block.timestamp,
                'transactions': block.transactions,
                'previous_hash': block.previous_hash,
                'hash': block.hash,
                'nonce': block.nonce,
                'shard_id': block.shard_id
            }
        return block  # If already a dict, return as is

    async def _send_message(self, writer, message: dict):
        """Send a message to the connection"""
        try:
            # Convert to JSON and encode
            data = json.dumps(message).encode()
            
            # Send message length first (4 bytes)
            writer.write(len(data).to_bytes(4, 'big'))
            
            # Send message data
            writer.write(data)
            await writer.drain()
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise

    async def connect_to_peer(self, host: str, port: int):
        """Connect to a peer"""
        peer_id = f"node_{self.shard_id}_{port - 5000}"  # Derive peer_id from port
        
        # Skip if already connected or connecting
        if peer_id in self.peer_states and self.peer_states[peer_id] in ["connected", "connecting"]:
            return
            
        try:
            self.peer_states[peer_id] = "connecting"
            
            # Skip if already connected
            if peer_id in self.peers:
                reader, writer = self.peers[peer_id]
                if not writer.is_closing():
                    return
                    
            # Connect and send hello message
            reader, writer = await asyncio.open_connection(host, port)
            await self._send_message(writer, {
                "type": MessageType.HELLO.value,
                "data": {
                    "node_id": self.node_id,
                    "shard_id": self.shard_id,
                    "host": self.host,
                    "port": self.port
                }
            })
            
            # Wait for hello response
            response = await self._read_message(reader)
            if not response or response["type"] != MessageType.HELLO.value:
                writer.close()
                await writer.wait_closed()
                self.peer_states[peer_id] = "disconnected"
                return
                
            # Store connection
            self.peers[peer_id] = (host, port)
            self.peer_states[peer_id] = "connected"
            logger.info(f"Connected to peer {peer_id}")
            
        except Exception as e:
            logger.warning(f"Failed to connect to peer {host}:{port}: {e}")
            self.peer_states[peer_id] = "disconnected"
            raise

    async def discover_peers(self):
        """Discover peers through bootstrap nodes"""
        self.logger.info(f"{self.node_id} discovering peers")
        
        # Calculate port range for nodes in our shard
        shard_start_port = 5000 + (self.shard_id * 2)
        shard_end_port = shard_start_port + 2
        
        # Try to connect to all nodes in our shard with retries
        max_retries = 3
        retry_delay = 1.0
        
        for port in range(shard_start_port, shard_end_port):
            # Don't connect to ourselves
            if port != self.port:
                for retry in range(max_retries):
                    try:
                        # Add timeout to connection attempt
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection("127.0.0.1", port),
                            timeout=2.0
                        )
                        
                        # Send hello message with our genesis block hash
                        await self._send_message(writer, {
                            "type": MessageType.HELLO.value,
                            "data": {
                                "node_id": self.node_id,
                                "shard_id": self.shard_id,
                                "host": self.host,
                                "port": self.port,
                                "genesis_hash": self.get_genesis_hash() if self.get_genesis_hash else None
                            }
                        })
                        
                        # Wait for response with timeout
                        try:
                            response = await asyncio.wait_for(
                                self._read_message(reader),
                                timeout=2.0
                            )
                            if response and response["type"] == MessageType.HELLO.value:
                                peer_id = response["data"]["node_id"]
                                peer_shard = response["data"]["shard_id"]
                                peer_genesis_hash = response["data"].get("genesis_hash")
                                
                                # Verify shard and genesis block
                                if peer_shard == self.shard_id:
                                    if peer_genesis_hash and self.get_genesis_hash and peer_genesis_hash != self.get_genesis_hash():
                                        self.logger.error(f"Genesis block mismatch with peer {peer_id}")
                                        writer.close()
                                        await writer.wait_closed()
                                        break
                                        
                                    # Store connection
                                    self.connections[peer_id] = (reader, writer)
                                    self.logger.info(f"Connected to peer {peer_id}")
                                    
                                    # Immediately sync chains
                                    await self._sync_chains_with_peer(peer_id, reader, writer)
                                    break  # Successfully connected, no need for more retries
                            else:
                                self.logger.warning(f"Invalid response from 127.0.0.1:{port}")
                                writer.close()
                                await writer.wait_closed()
                                
                        except asyncio.TimeoutError:
                            self.logger.warning(f"Timeout waiting for response from 127.0.0.1:{port}")
                            writer.close()
                            await writer.wait_closed()
                            
                    except asyncio.TimeoutError:
                        self.logger.warning(f"Timeout connecting to 127.0.0.1:{port}")
                    except ConnectionRefusedError:
                        self.logger.warning(f"Connection refused to 127.0.0.1:{port}")
                    except Exception as e:
                        self.logger.warning(f"Failed to connect to 127.0.0.1:{port}: {e}")
                    
                    if retry < max_retries - 1:
                        await asyncio.sleep(retry_delay)
        
        # Wait a bit for connections to stabilize
        await asyncio.sleep(2)
        
        # Log discovered peers in our shard
        shard_peers = [peer_id for peer_id in self.connections.keys() 
                      if peer_id.startswith(f"node_{self.shard_id}_")]
        self.logger.info(f"{self.node_id} discovered {len(shard_peers)} peers in shard {self.shard_id}: {shard_peers}")
        
        if not shard_peers:
            self.logger.warning(f"No peers found in shard {self.shard_id}")
            # Try one more time with a longer delay
            await asyncio.sleep(3)
            await self._retry_peer_discovery()

    async def _retry_peer_discovery(self):
        """Retry peer discovery with longer timeouts"""
        logger.info(f"{self.node_id} retrying peer discovery")
        
        # Calculate port range for nodes in our shard
        shard_start_port = 5000 + (self.shard_id * 2)
        shard_end_port = shard_start_port + 2
        
        for port in range(shard_start_port, shard_end_port):
            if port != self.port:
                try:
                    # Use longer timeout for retry
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", port),
                        timeout=5.0
                    )
                    
                    # Send hello message
                    await self._send_message(writer, {
                        "type": MessageType.HELLO.value,
                        "data": {
                            "node_id": self.node_id,
                            "shard_id": self.shard_id,
                            "host": self.host,
                            "port": self.port,
                            "genesis_hash": self.get_genesis_hash() if self.get_genesis_hash else None
                        }
                    })
                    
                    # Wait for response with longer timeout
                    response = await asyncio.wait_for(
                        self._read_message(reader),
                        timeout=5.0
                    )
                    
                    if response and response["type"] == MessageType.HELLO.value:
                        peer_id = response["data"]["node_id"]
                        peer_shard = response["data"]["shard_id"]
                        
                        if peer_shard == self.shard_id:
                            self.peers[peer_id] = (reader, writer)
                            self.peer_states[peer_id] = "connected"
                            logger.info(f"Successfully connected to peer {peer_id} on retry")
                            
                            # Sync chains
                            await self._sync_chains_with_peer(peer_id, reader, writer)
                            
                except ConnectionRefusedError:
                    logger.warning(f"Connection refused to 127.0.0.1:{port}")
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout connecting to 127.0.0.1:{port}")
                except Exception as e:
                    logger.warning(f"Retry connection failed for port {port}: {e}")

    async def broadcast_to_shard(self, message: dict, shard_id: int, max_retries: int = 3):
        """Broadcast message to peers in the same shard with retries"""
        sent_to = set()
        retry_count = 0
        
        while retry_count < max_retries:
            # Get list of peers in the same shard
            target_peers = [peer_id for peer_id in self.peers.keys() 
                           if peer_id.startswith(f"node_{shard_id}_") and peer_id != self.node_id]
            
            if not target_peers:
                logger.warning(f"No peers found in shard {shard_id}")
                return
            
            # Try to send to each peer
            for peer_id in target_peers:
                if peer_id in sent_to:
                    continue
                
                try:
                    reader, writer = self.peers[peer_id]
                    if writer.is_closing():
                        # Try to reconnect
                        host, port = self.peers[peer_id]
                        try:
                            reader, writer = await asyncio.open_connection(host, port)
                            self.peers[peer_id] = (reader, writer)
                        except Exception as e:
                            logger.warning(f"Failed to reconnect to {peer_id}: {e}")
                            continue
                        
                    await self._send_message(writer, message)
                    logger.info(f"Broadcasted message to {peer_id}")
                    sent_to.add(peer_id)
                    
                except Exception as e:
                    logger.warning(f"Failed to broadcast to {peer_id}: {e}")
                    # Try to reconnect on next retry
                    if peer_id in self.peers:
                        del self.peers[peer_id]
            
            # If we've sent to all peers, we're done
            if len(sent_to) == len(target_peers):
                break
            
            # Wait before retrying
            retry_count += 1
            if retry_count < max_retries:
                logger.info(f"Retrying broadcast to remaining peers (attempt {retry_count + 1})")
                await asyncio.sleep(1)
            
        if len(sent_to) < len(target_peers):
            logger.warning(f"Failed to broadcast to all peers in shard {shard_id}. Sent to {len(sent_to)}/{len(target_peers)} peers")

    async def broadcast_transaction(self, transaction: dict):
        """Broadcast transaction to peers in the same shard"""
        message = {
            "type": MessageType.TRANSACTION.value,
            "data": transaction
        }
        await self.broadcast_to_shard(message, self.shard_id)

    async def broadcast_block(self, block: dict):
        """Broadcast block to peers in the same shard"""
        try:
            # Serialize block if it's an object
            block_data = self._serialize_block(block)
            
            message = {
                "type": MessageType.BLOCK.value,
                "data": block_data
            }
            await self.broadcast_to_shard(message, self.shard_id)
        except Exception as e:
            logger.error(f"Error broadcasting block: {e}")

    async def broadcast_message(self, message: dict):
        """Broadcast message to all peers in the same shard"""
        tasks = []
        for peer_id, (reader, writer) in self.peers.items():
            if peer_id.startswith(f"node_{self.shard_id}_"):
                try:
                    await self._send_message(writer, message)
                    logger.info(f"Broadcasted message to {peer_id}")
                except Exception as e:
                    logger.error(f"Error broadcasting to {peer_id}: {e}")
        return tasks 