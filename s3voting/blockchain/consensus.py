from typing import Dict, List, Optional, Tuple
import asyncio
import random
import time
import logging
from dataclasses import dataclass
from enum import Enum
from .types import Block, Transaction

logger = logging.getLogger(__name__)

class LeaderRole(Enum):
    MAIN = "main"
    BACKUP = "backup"

@dataclass
class Leader:
    node_id: str
    role: LeaderRole
    reputation: int = 1  # 1 for good, 0 for bad

class ConsensusState:
    def __init__(self):
        self.current_slot = 0
        self.epoch = 0
        self.leaders: Dict[int, Tuple[Leader, Leader]] = {}  # shard_id -> (m_leader, b_leader)
        self.miner_reputations: Dict[str, int] = {}  # node_id -> reputation
        self.pending_signatures: Dict[str, List[Dict]] = {}  # block_hash -> signatures
        self.raw_blocks: Dict[str, Dict] = {}  # block_hash -> raw_block
        self.consensus_timeout = 5.0  # seconds to wait for consensus

class ConsensusManager:
    def __init__(self, shard_id: int):
        self.shard_id = shard_id
        self.current_slot = 0
        self.current_epoch = 0
        self.main_leader = None
        self.backup_leader = None
        self.miner_reputations = {}  # node_id -> reputation
        self.pending_signatures = {}  # block_hash -> {node_id -> signature}
        self.raw_blocks = {}  # block_hash -> raw_block
        self.miners = []  # List of registered miners
        self.logger = logging.getLogger(f"s3voting.blockchain.consensus.shard_{shard_id}")
        
        # Initialize with default reputation for all nodes
        self._initialize_miner_reputations()
    
    def _initialize_miner_reputations(self):
        """Initialize reputations for all nodes in the shard"""
        # Calculate node IDs for this shard
        base_node_id = self.shard_id * 2
        for i in range(2):  # Each shard has 2 nodes
            node_id = f"node_{base_node_id + i}"
            self.miner_reputations[node_id] = 1.0  # Start with full reputation
            self.miners.append(node_id)  # Add to miners list
            self.logger.info(f"Initialized reputation for {node_id}: 1.0")
    
    def register_miner(self, node_id: str):
        """Register a new miner"""
        if node_id not in self.miner_reputations:
            self.miner_reputations[node_id] = 1.0
            self.miners.append(node_id)  # Add to miners list
            self.logger.info(f"Registered new miner {node_id} with initial reputation 1.0")
    
    def get_eligible_miners(self) -> List[str]:
        """Get list of miners with good reputation"""
        eligible = [node_id for node_id, rep in self.miner_reputations.items() 
                   if rep >= 0.5]  # Consider miners with reputation >= 0.5 as eligible
        if not eligible:
            self.logger.warning(f"No eligible miners in shard {self.shard_id}")
        return eligible
    
    def select_leaders(self) -> Tuple[Optional[Leader], Optional[Leader]]:
        """Select main and backup leaders based on reputation"""
        eligible_miners = self.get_eligible_miners()
        
        if len(eligible_miners) < 2:
            self.logger.warning(f"Not enough eligible miners in shard {self.shard_id}")
            return None, None
        
        # Randomly select two different miners
        selected = random.sample(eligible_miners, 2)
        
        self.main_leader = Leader(
            node_id=selected[0],
            role=LeaderRole.MAIN,
            reputation=self.miner_reputations[selected[0]]
        )
        
        self.backup_leader = Leader(
            node_id=selected[1],
            role=LeaderRole.BACKUP,
            reputation=self.miner_reputations[selected[1]]
        )
        
        self.logger.info(f"Selected leaders for shard {self.shard_id}: "
                        f"Main={self.main_leader.node_id}, "
                        f"Backup={self.backup_leader.node_id}")
        
        return self.main_leader, self.backup_leader

    async def propose_block(self, node_id: str, raw_block: Dict) -> bool:
        """Propose a raw block as the main leader"""
        m_leader, b_leader = self.state.leaders.get(self.shard_id, (None, None))
        if not m_leader or m_leader.node_id != node_id:
            logger.warning(f"Node {node_id} is not the main leader")
            return False

        block_hash = raw_block["hash"]
        self.state.raw_blocks[block_hash] = raw_block
        self.state.pending_signatures[block_hash] = []
        self._consensus_locks[block_hash] = asyncio.Lock()
        
        logger.info(f"M-Leader {node_id} proposed block {block_hash[:8]}")
        return True

    async def sign_block(self, node_id: str, block_hash: str, signature: Dict) -> bool:
        """Sign a proposed block"""
        if block_hash not in self.state.pending_signatures:
            logger.warning(f"Block {block_hash[:8]} not found")
            return False

        async with self._consensus_locks[block_hash]:
            self.state.pending_signatures[block_hash].append({
                "node_id": node_id,
                "signature": signature,
                "timestamp": time.time()
            })
            
            # Check if we have enough signatures
            if len(self.state.pending_signatures[block_hash]) > len(self.miners) // 2:
                logger.info(f"Block {block_hash[:8]} has enough signatures")
                return True
        return False

    async def finalize_block(self, node_id: str, block_hash: str) -> Optional[Dict]:
        """Finalize a block with collected signatures"""
        m_leader, b_leader = self.state.leaders.get(self.shard_id, (None, None))
        if not m_leader and not b_leader:
            logger.warning("No leaders available")
            return None

        if block_hash not in self.state.raw_blocks:
            logger.warning(f"Block {block_hash[:8]} not found")
            return None

        async with self._consensus_locks[block_hash]:
            signatures = self.state.pending_signatures.get(block_hash, [])
            if len(signatures) <= len(self.miners) // 2:
                logger.warning(f"Not enough signatures for block {block_hash[:8]}")
                return None

            # Create final block
            raw_block = self.state.raw_blocks[block_hash]
            final_block = {
                **raw_block,
                "signatures": signatures,
                "finalized_by": node_id
            }

            # Update reputations if backup leader finalized
            if node_id == b_leader.node_id:
                self.state.miner_reputations[m_leader.node_id] = 0
                logger.warning(f"M-Leader {m_leader.node_id} reputation set to 0")

            # Cleanup
            del self.state.raw_blocks[block_hash]
            del self.state.pending_signatures[block_hash]
            del self._consensus_locks[block_hash]

            logger.info(f"Block {block_hash[:8]} finalized by {node_id}")
            return final_block

    async def handle_leader_failure(self, node_id: str, block_hash: str):
        """Handle leader failure and switch to backup leader"""
        m_leader, b_leader = self.state.leaders.get(self.shard_id, (None, None))
        if not m_leader or not b_leader:
            logger.warning("No leaders available")
            return

        if node_id == m_leader.node_id:
            logger.warning(f"M-Leader {node_id} failed, switching to B-Leader {b_leader.node_id}")
            # Backup leader takes over
            if block_hash in self.state.raw_blocks:
                # M-Leader proposed but didn't finalize
                await self.finalize_block(b_leader.node_id, block_hash)
            else:
                # M-Leader didn't even propose
                self.state.miner_reputations[m_leader.node_id] = 0
                logger.warning(f"M-Leader {m_leader.node_id} reputation set to 0")

    def update_reputations(self):
        """Update miner reputations at the end of an epoch"""
        for node_id, rep in self.state.miner_reputations.items():
            if rep == 0:
                logger.info(f"Miner {node_id} has bad reputation")
            else:
                logger.info(f"Miner {node_id} has good reputation")

    def get_eligible_miners(self) -> List[str]:
        """Get list of miners with good reputation"""
        return [m for m in self.miners if self.state.miner_reputations[m] == 1] 