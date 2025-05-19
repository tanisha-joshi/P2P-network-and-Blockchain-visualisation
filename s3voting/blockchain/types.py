from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

class MessageType(Enum):
    HELLO = "HELLO"
    PEER_LIST = "PEER_LIST"
    TRANSACTION = "TRANSACTION"
    BLOCK = "BLOCK"
    SYNC_REQUEST = "SYNC_REQUEST"
    SYNC_RESPONSE = "SYNC_RESPONSE"
    SIGNATURE = "SIGNATURE"

@dataclass
class Block:
    index: int
    timestamp: float
    transactions: List[dict]
    previous_hash: str
    hash: str
    nonce: int = 0
    shard_id: int = 0

    def to_dict(self) -> dict:
        return {
            'index': self.index,
            'timestamp': self.timestamp,
            'transactions': self.transactions,
            'previous_hash': self.previous_hash,
            'hash': self.hash,
            'nonce': self.nonce,
            'shard_id': self.shard_id
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Block':
        return cls(
            index=data['index'],
            timestamp=data['timestamp'],
            transactions=data['transactions'],
            previous_hash=data['previous_hash'],
            hash=data['hash'],
            nonce=data.get('nonce', 0),
            shard_id=data.get('shard_id', 0)
        )

@dataclass
class Transaction:
    sender: str
    recipient: str
    amount: float
    timestamp: float
    signature: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'sender': self.sender,
            'recipient': self.recipient,
            'amount': self.amount,
            'timestamp': self.timestamp,
            'signature': self.signature
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Transaction':
        return cls(
            sender=data['sender'],
            recipient=data['recipient'],
            amount=data['amount'],
            timestamp=data['timestamp'],
            signature=data.get('signature')
        )

    def verify(self) -> bool:
        # In a real implementation, this would verify the signature
        return True 