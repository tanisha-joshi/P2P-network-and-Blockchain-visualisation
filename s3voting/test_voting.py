import asyncio
from s3voting.voting.vote_manager import VoteManager
from s3voting.blockchain.node import BlockchainNode
from web3 import Web3
import logging
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_key_pair():
    """Generate RSA key pair"""
    key = RSA.generate(2048)
    return key, key.publickey()

def sign_message(private_key: RSA.RsaKey, message: str) -> str:
    """Sign a message using RSA private key"""
    h = SHA256.new(message.encode())
    signature = pkcs1_15.new(private_key).sign(h)
    return signature.hex()

async def test_voting_system():
    # Initialize Web3 connection
    web3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
    
    # Initialize vote manager
    vote_manager = VoteManager()
    
    # Generate key pairs for voters
    voter_keys = {}
    for i in range(1, 5):
        voter_id = f"voter{i}"
        private_key, public_key = generate_key_pair()
        voter_keys[voter_id] = private_key
        vote_manager.register_voter(voter_id, public_key.export_key().decode())
        logger.info(f"Registered voter: {voter_id}")

    # Define votes
    votes = [
        ("voter1", "candidate1"),
        ("voter2", "candidate2"),
        ("voter3", "candidate1"),
        ("voter4", "candidate2")
    ]
    
    # Submit votes
    logger.info("\nSubmitting votes...")
    for voter_id, choice in votes:
        # Sign the vote with voter's private key
        signature = sign_message(voter_keys[voter_id], choice)
        success, shard_id = vote_manager.submit_ballot(
            f"vote_{voter_id}",
            voter_id,
            choice,
            signature,
            1234567890  # timestamp
        )
        if success:
            logger.info(f"Vote from {voter_id} for {choice} submitted to shard {shard_id}")
        else:
            logger.error(f"Failed to submit vote from {voter_id}")

    # Get results per shard
    logger.info("\nResults by shard:")
    for shard_id in range(vote_manager.num_shards):
        results = vote_manager.get_shard_results(shard_id)
        logger.info(f"Shard {shard_id} results: {results}")

    # Get total results
    logger.info("\nTotal results:")
    total_results = vote_manager.get_election_results()
    logger.info(f"All shards combined: {total_results}")

    # Show voter distribution across shards
    logger.info("\nVoter distribution across shards:")
    for voter_id, _ in votes:
        shard_id = vote_manager._get_shard_id(voter_id)
        logger.info(f"Voter {voter_id} belongs to shard {shard_id}")

if __name__ == "__main__":
    asyncio.run(test_voting_system()) 