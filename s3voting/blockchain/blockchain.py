import asyncio
import time
import logging
from typing import Optional, Dict, List
from .types import Block, Transaction

logger = logging.getLogger(__name__)

class Blockchain:
    def __init__(self, shard_id: int):
        self.chain = []
        self.pending_transactions = []
        self.shard_id = shard_id
        self.difficulty = 4
        self.mining_reward = 10
        self._lock = asyncio.Lock()

    async def create_block(self) -> Optional[Block]:
        """Create a new block with pending transactions"""
        if not self.pending_transactions:
            return None

        async with self._lock:
            previous_hash = self.chain[-1].hash if self.chain else "0" * 64
            block = Block(
                index=len(self.chain),
                timestamp=time.time(),
                transactions=self.pending_transactions.copy(),
                previous_hash=previous_hash,
                shard_id=self.shard_id
            )

            # Clear pending transactions
            self.pending_transactions = []
            return block

    async def add_block(self, block: Block) -> bool:
        """Add a block to the chain"""
        async with self._lock:
            if not self.verify_block(block):
                return False

            self.chain.append(block)
            return True

    async def sign_block(self, block_hash: str) -> Optional[Dict]:
        """Sign a block with the node's private key"""
        try:
            # In a real implementation, this would use proper cryptographic signing
            # For now, we'll just create a mock signature
            signature = {
                "signature": f"mock_signature_{block_hash}",
                "timestamp": time.time()
            }
            return signature
        except Exception as e:
            logger.error(f"Error signing block: {e}")
            return None

    def verify_block(self, block: Block) -> bool:
        """Verify a block's validity"""
        if not self.chain:
            return block.previous_hash == "0" * 64

        previous_block = self.chain[-1]
        return (
            block.previous_hash == previous_block.hash and
            block.index == previous_block.index + 1 and
            block.shard_id == self.shard_id
        )

    def get_chain(self) -> List[Block]:
        """Get the current chain"""
        return self.chain.copy()

    def get_latest_block(self) -> Optional[Block]:
        """Get the latest block in the chain"""
        return self.chain[-1] if self.chain else None

    def add_transaction(self, transaction: Transaction) -> bool:
        """Add a transaction to the pending transactions"""
        if not transaction.verify():
            return False

        self.pending_transactions.append(transaction)
        return True

    def get_balance(self, address: str) -> float:
        """Get the balance of an address"""
        balance = 0.0

        for block in self.chain:
            for tx in block.transactions:
                if tx.sender == address:
                    balance -= tx.amount
                if tx.recipient == address:
                    balance += tx.amount

        return balance

    def to_dict(self) -> Dict:
        """Convert the blockchain to a dictionary"""
        return {
            "chain": [block.to_dict() for block in self.chain],
            "pending_transactions": [tx.to_dict() for tx in self.pending_transactions],
            "shard_id": self.shard_id
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Blockchain':
        """Create a blockchain from a dictionary"""
        blockchain = cls(data["shard_id"])
        blockchain.chain = [Block.from_dict(block_data) for block_data in data["chain"]]
        blockchain.pending_transactions = [
            Transaction.from_dict(tx_data) for tx_data in data["pending_transactions"]
        ]
        return blockchain 