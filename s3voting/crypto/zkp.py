from typing import Tuple
from Crypto.Util.number import getPrime, bytes_to_long, long_to_bytes
import random
import hashlib

class ZKP:
    def __init__(self, p: int = None, g: int = None):
        """Initialize the ZKP system with prime p and generator g"""
        if p is None or g is None:
            # Generate safe prime and generator
            self.p = getPrime(1024)
            self.g = self._find_generator(self.p)
        else:
            self.p = p
            self.g = g

    def _find_generator(self, p: int) -> int:
        """Find a generator for the multiplicative group modulo p"""
        factors = self._factor(p-1)
        for g in range(2, p):
            if all(pow(g, (p-1)//f, p) != 1 for f in factors):
                return g
        raise ValueError("No generator found")

    def _factor(self, n: int) -> list:
        """Factor a number into its prime factors"""
        factors = []
        while n % 2 == 0:
            factors.append(2)
            n = n // 2
        i = 3
        while i * i <= n:
            while n % i == 0:
                factors.append(i)
                n = n // i
            i += 2
        if n > 2:
            factors.append(n)
        return factors

    def generate_proof(self, secret: int, public_value: int) -> Tuple[int, int]:
        """Generate a zero-knowledge proof for a secret value"""
        r = random.randint(1, self.p-2)
        t = pow(self.g, r, self.p)
        
        # Create challenge
        c = self._hash_values(t, public_value)
        
        # Create response
        s = (r + c * secret) % (self.p-1)
        
        return (t, s)

    def verify_proof(self, proof: Tuple[int, int], public_value: int) -> bool:
        """Verify a zero-knowledge proof"""
        t, s = proof
        
        # Recreate challenge
        c = self._hash_values(t, public_value)
        
        # Verify the proof
        lhs = pow(self.g, s, self.p)
        rhs = (t * pow(public_value, c, self.p)) % self.p
        
        return lhs == rhs

    def _hash_values(self, *values) -> int:
        """Hash multiple values to create a challenge"""
        h = hashlib.sha256()
        for value in values:
            h.update(str(value).encode())
        return int(h.hexdigest(), 16) % self.p

class VoterZKP:
    def __init__(self):
        self.zkp = ZKP()
        self.voter_secrets = {}

    def register_voter(self, voter_id: str) -> Tuple[int, int]:
        """Register a voter and generate their secret/public key pair"""
        secret = random.randint(1, self.zkp.p-2)
        public = pow(self.zkp.g, secret, self.zkp.p)
        self.voter_secrets[voter_id] = secret
        return (secret, public)

    def generate_vote_proof(self, voter_id: str, vote: int) -> Tuple[int, int]:
        """Generate a proof that the voter is eligible to vote"""
        if voter_id not in self.voter_secrets:
            raise ValueError("Voter not registered")
        
        secret = self.voter_secrets[voter_id]
        public = pow(self.zkp.g, secret, self.zkp.p)
        return self.zkp.generate_proof(secret, public) 