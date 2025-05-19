from typing import Dict, List, Optional
from dataclasses import dataclass
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import json
import hashlib

@dataclass
class Voter:
    id: str
    public_key: str
    registered: bool
    voted: bool

@dataclass
class Ballot:
    vote_id: str
    voter_id: str
    choice: str
    signature: str
    timestamp: int

class VoteManager:
    def __init__(self):
        self.voters: Dict[str, Voter] = {}
        self.ballots: Dict[str, Ballot] = {}
        self.election_state: Dict = {}
        self.num_shards: int = 2  # Default to 2 shards

    def register_voter(self, voter_id: str, public_key: str) -> bool:
        """Register a new voter with their public key"""
        if voter_id in self.voters:
            return False
        
        self.voters[voter_id] = Voter(
            id=voter_id,
            public_key=public_key,
            registered=True,
            voted=False
        )
        return True

    def _get_shard_id(self, voter_id: str) -> int:
        """Determine which shard a voter belongs to based on their ID"""
        # Simple hash-based sharding
        hash_value = int(hashlib.sha256(voter_id.encode()).hexdigest(), 16)
        return hash_value % self.num_shards

    def submit_ballot(self, vote_id: str, voter_id: str, choice: str, signature: str, timestamp: int) -> tuple[bool, int]:
        """Submit a new ballot with verification and shard routing"""
        if voter_id not in self.voters or not self.voters[voter_id].registered:
            return False, -1
        
        if self.voters[voter_id].voted:
            return False, -1

        # Verify signature
        if not self._verify_signature(voter_id, choice, signature):
            return False, -1

        # Get the shard ID for this voter
        shard_id = self._get_shard_id(voter_id)
        
        # Create ballot with shard information
        self.ballots[vote_id] = Ballot(
            vote_id=vote_id,
            voter_id=voter_id,
            choice=choice,
            signature=signature,
            timestamp=timestamp
        )

        self.voters[voter_id].voted = True
        
        # Return the shard ID along with success status
        return True, shard_id

    def _verify_signature(self, voter_id: str, message: str, signature: str) -> bool:
        """Verify the signature of a ballot"""
        try:
            voter = self.voters[voter_id]
            public_key = RSA.import_key(voter.public_key)
            h = SHA256.new(message.encode())
            verifier = pkcs1_15.new(public_key)
            verifier.verify(h, bytes.fromhex(signature))
            return True
        except (ValueError, TypeError):
            return False

    def get_election_results(self) -> Dict:
        """Get the current election results"""
        results = {}
        for ballot in self.ballots.values():
            if ballot.choice not in results:
                results[ballot.choice] = 0
            results[ballot.choice] += 1
        return results

    def verify_voter_eligibility(self, voter_id: str) -> bool:
        """Check if a voter is eligible to vote"""
        return voter_id in self.voters and self.voters[voter_id].registered and not self.voters[voter_id].voted 

    def get_shard_results(self, shard_id: int) -> Dict:
        """Get election results for a specific shard"""
        results = {}
        for ballot in self.ballots.values():
            if self._get_shard_id(ballot.voter_id) == shard_id:
                if ballot.choice not in results:
                    results[ballot.choice] = 0
                results[ballot.choice] += 1
        return results 