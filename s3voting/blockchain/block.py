from typing import Dict, List
import hashlib
import json

class Block:
    def __init__(self, index: int, timestamp: float, transactions: List[Dict], 
                 previous_hash: str, hash: str, nonce: int, shard_id: int):
        self.index = index
        self.timestamp = timestamp
        self.transactions = transactions
        self.previous_hash = previous_hash
        self.hash = hash
        self.nonce = nonce
        self.shard_id = shard_id
        
    def calculate_hash(self) -> str:
        """Calculate block hash"""
        block_data = {
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": self.transactions,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "shard_id": self.shard_id
        }
        block_string = json.dumps(block_data, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()
        
    def verify_hash(self) -> bool:
        """Verify block hash"""
        return self.hash == self.calculate_hash()
        
    def to_dict(self) -> Dict:
        """Convert block to dictionary"""
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": self.transactions,
            "previous_hash": self.previous_hash,
            "hash": self.hash,
            "nonce": self.nonce,
            "shard_id": self.shard_id
        } 