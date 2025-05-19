from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional, List
import time
import asyncio
import logging
from web3 import Web3
from Crypto.PublicKey import RSA

from s3voting.voting.vote_manager import VoteManager
from s3voting.crypto.zkp import VoterZKP
from s3voting.blockchain.shard import ShardManager
from s3voting.blockchain.node import BlockchainNode

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="S3Voting API")

# Global state
election_state = {
    "is_active": False,
    "phase": "setup",  # setup, registration, voting, results
    "nodes": [],
    "shard_manager": None,
    "vote_manager": None,
    "web3": None,
    "registered_voters": set(),
    "votes_cast": set(),
    "results": {},
    "election_name": "",
    "candidates": []
}

class ElectionSetup(BaseModel):
    num_shards: int
    nodes_per_shard: int
    election_name: str
    candidates: List[str]

class VoterRegistration(BaseModel):
    voter_id: str

class VoteSubmission(BaseModel):
    voter_id: str
    choice: str
    signature: str
    proof: Dict[str, int]

@app.post("/setup-election")
async def setup_election(setup: ElectionSetup):
    """Initialize the election system"""
    try:
        # Validate input parameters
        if setup.num_shards < 1 or setup.nodes_per_shard < 1:
            raise ValueError("Number of shards and nodes per shard must be positive")
        if not setup.election_name or not setup.candidates:
            raise ValueError("Election name and candidates list cannot be empty")

        # Initialize Web3
        election_state["web3"] = Web3()
        
        # Initialize shard manager
        election_state["shard_manager"] = ShardManager(num_shards=setup.num_shards)
        
        # Initialize vote manager
        election_state["vote_manager"] = VoteManager()
        
        # Create nodes for each shard
        nodes = []
        for shard_id in range(setup.num_shards):
            for node_num in range(setup.nodes_per_shard):
                node_id = f"node_shard{shard_id}_{node_num}"
                port = 5000 + (shard_id * setup.nodes_per_shard) + node_num
                node = BlockchainNode(
                    node_id=node_id,
                    shard_id=shard_id,
                    web3=election_state["web3"],
                    host="127.0.0.1",
                    port=port
                )
                nodes.append(node)
        
        # Start all nodes
        try:
            start_tasks = [node.start() for node in nodes]
            await asyncio.gather(*start_tasks)
            logger.info("All nodes started successfully")
        except Exception as e:
            # Clean up any started nodes
            for node in nodes:
                await node.stop()
            raise Exception(f"Failed to start nodes: {str(e)}")
        # Discover peers with proper error handling and retries
        try:
            discover_tasks = []

            for node in nodes:
                discover_tasks.append(node.discover_peers())
                
            
            
            # Wait for all discovery tasks to complete
            await asyncio.gather(*discover_tasks)
            logger.info("Peer discovery completed successfully")
            
            # Verify peer connections
            for node in nodes:
                connected_peers = len(node.p2p.connections)
                if connected_peers == 0:
                    logger.warning(f"Node {node.node_id} has no peer connections")
                    # Retry discovery for this node
                    await node.discover_peers()
            
        except Exception as e:
            # Clean up on peer discovery failure
            for node in nodes:
                await node.stop()
            raise Exception(f"Peer discovery failed: {str(e)}")

        # Update election state
        election_state["nodes"] = nodes
        election_state["is_active"] = True
        election_state["phase"] = "registration"
        election_state["election_name"] = setup.election_name
        election_state["candidates"] = setup.candidates
        
        return {
            "status": "success",
            "message": "Election system initialized successfully",
            "num_shards": setup.num_shards,
            "total_nodes": len(nodes),
            "election_name": setup.election_name
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/register")
async def register_voter(registration: VoterRegistration):
    """Register a new voter"""
    if election_state["phase"] != "registration":
        raise HTTPException(status_code=400, detail="Registration phase is not active")
    
    if registration.voter_id in election_state["registered_voters"]:
        raise HTTPException(status_code=400, detail="Voter already registered")
    
    # Generate RSA key pair for the voter
    key = RSA.generate(2048)
    public_key = key.publickey().export_key().decode()
    
    success = election_state["vote_manager"].register_voter(
        registration.voter_id,
        public_key
    )
    
    if success:
        election_state["registered_voters"].add(registration.voter_id)
        return {
            "status": "success", 
            "message": "Voter registered successfully",
            "public_key": public_key
        }
    else:
        raise HTTPException(status_code=400, detail="Failed to register voter")

@app.post("/start-voting")
async def start_voting():
    """Start the voting phase"""
    if election_state["phase"] != "registration":
        raise HTTPException(status_code=400, detail="Can only start voting after registration")
    
    election_state["phase"] = "voting"
    return {"status": "success", "message": "Voting phase started"}

@app.post("/vote")
async def submit_vote(vote: VoteSubmission):
    """Submit a vote"""
    try:
        if election_state["phase"] != "voting":
            raise HTTPException(status_code=400, detail="Voting phase is not active")
        
        if vote.voter_id not in election_state["registered_voters"]:
            raise HTTPException(status_code=400, detail="Voter not registered")
        
        if vote.voter_id in election_state["votes_cast"]:
            raise HTTPException(status_code=400, detail="Voter has already cast their vote")
            
        if vote.choice not in election_state["candidates"]:
            raise HTTPException(status_code=400, detail="Invalid candidate choice")

        # Create vote transaction
        vote_transaction = {
            "type": "vote",
            "voter_id": vote.voter_id,
            "candidate_id": vote.choice,
            "signature": vote.signature,
            "proof": vote.proof,
            "timestamp": int(time.time())
        }

        # Get shard assignment for this vote
        vote_shard = election_state["shard_manager"].get_shard_for_vote(vote.voter_id)
        vote_transaction["shard_id"] = vote_shard

        # Send vote to appropriate shard nodes
        success = False
        error_messages = []
        
        for node in election_state["nodes"]:
            if node.shard_id == vote_shard:
                try:
                    await node.add_transaction(vote_transaction)
                    # Wait for transaction to be mined
                    await asyncio.sleep(1)  # Give time for transaction to be processed
                    success = True
                    logger.info(f"Vote from {vote.voter_id} added to node {node.node_id} in shard {vote_shard}")
                except Exception as e:
                    error_msg = f"Failed to add transaction to node {node.node_id}: {str(e)}"
                    logger.error(error_msg)
                    error_messages.append(error_msg)
                    continue

        if success:
            # Record that this voter has cast their vote
            election_state["votes_cast"].add(vote.voter_id)
            logger.info(f"Vote from {vote.voter_id} successfully recorded")
            
            return {
                "status": "success",
                "message": "Vote submitted successfully",
                "shard_id": vote_shard
            }
        else:
            error_detail = "Failed to submit vote to blockchain nodes. Errors: " + "; ".join(error_messages)
            logger.error(error_detail)
            raise HTTPException(status_code=500, detail=error_detail)
            
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error submitting vote: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to submit vote: {str(e)}")

@app.post("/end-voting")
async def end_voting():
    """End the voting phase and calculate results"""
    try:
        if election_state["phase"] != "voting":
            raise HTTPException(status_code=400, detail="Voting phase is not active")
        
        if not election_state["nodes"]:
            raise HTTPException(status_code=500, detail="No nodes available for vote counting")
            
        if not election_state["candidates"]:
            raise HTTPException(status_code=500, detail="No candidates defined for the election")
        
        # Initialize vote tally for each candidate
        vote_tally = {candidate: 0 for candidate in election_state["candidates"]}
        
        # Only count votes from one node per shard to avoid double-counting
        counted_shards = set()
        total_votes = 0
        
        logger.info("Starting vote counting process...")
        logger.info(f"Number of nodes: {len(election_state['nodes'])}")
        
        for node in election_state["nodes"]:
            if node.shard_id in counted_shards:
                continue
            counted_shards.add(node.shard_id)
            
            logger.info(f"Counting votes from node {node.node_id} in shard {node.shard_id}")
            logger.info(f"Chain length: {len(node.chain)}")
            
            # Count votes from this node's chain
            for block in node.chain:
                logger.info(f"Processing block {block.index} with {len(block.transactions)} transactions")
                for vote in block.transactions:
                    if vote.get("type") == "vote":
                        candidate = vote.get("candidate_id")
                        if candidate in vote_tally:
                            vote_tally[candidate] += 1
                            total_votes += 1
                            logger.info(f"Counted vote from {vote.get('voter_id')} for {candidate}")
                        else:
                            logger.warning(f"Invalid candidate {candidate} in vote from {vote.get('voter_id')}")

        logger.info(f"Vote counting complete. Total votes: {total_votes}")
        logger.info(f"Vote tally: {vote_tally}")

        if total_votes == 0:
            logger.warning("No votes were counted in any shard")
            raise HTTPException(status_code=400, detail="No votes were counted in the election")

        # Determine winner
        winner = max(vote_tally.items(), key=lambda x: x[1])
        
        # Check for tie - a tie exists if all candidates have the same number of votes
        max_votes = winner[1]
        is_tie = all(votes == max_votes for votes in vote_tally.values())
        
        # Update election state
        election_state["results"] = vote_tally
        election_state["winner"] = winner[0]
        election_state["is_tie"] = is_tie
        election_state["total_votes"] = total_votes
        election_state["phase"] = "results"
        
        logger.info(f"Voting phase ended. Total votes: {total_votes}")
        logger.info(f"Vote tally: {vote_tally}")
        logger.info(f"Winner: {winner[0]} with {winner[1]} votes")
        if is_tie:
            logger.info("Note: There is a tie in the election results")

        return {
            "status": "success",
            "message": "Voting phase ended",
            "results": vote_tally,
            "winner": winner[0],
            "winner_votes": winner[1],
            "is_tie": is_tie,
            "total_votes": total_votes
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error ending voting phase: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to end voting phase: {str(e)}")

@app.get("/results")
async def get_results():
    """Get current election results"""
    if election_state["phase"] != "results":
        raise HTTPException(status_code=400, detail="Results are not yet available")
    
    return {
        "status": "success",
        "results": election_state["results"]
    }

@app.get("/election-status")
async def get_election_status():
    """Get current election status"""
    return {
        "is_active": election_state["is_active"],
        "phase": election_state["phase"],
        "election_name": election_state["election_name"],
        "candidates": election_state["candidates"],
        "registered_voters": len(election_state["registered_voters"]),
        "votes_cast": len(election_state["votes_cast"]),
        "num_shards": len(election_state["nodes"]) // 2 if election_state["nodes"] else 0,
        "total_nodes": len(election_state["nodes"])
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 