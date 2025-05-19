import streamlit as st
import requests
import json
from typing import List
import time

# API endpoint
API_URL = "http://localhost:8000"

def setup_election(num_shards: int, nodes_per_shard: int, election_name: str, candidates: List[str]):
    """Setup a new election"""
    response = requests.post(
        f"{API_URL}/setup-election",
        json={
            "num_shards": num_shards,
            "nodes_per_shard": nodes_per_shard,
            "election_name": election_name,
            "candidates": candidates
        }
    )
    return response.json()

def register_voter(voter_id: str):
    """Register a new voter"""
    response = requests.post(
        f"{API_URL}/register",
        json={
            "voter_id": voter_id
        }
    )
    return response.json()

def start_voting():
    """Start the voting phase"""
    response = requests.post(f"{API_URL}/start-voting")
    return response.json()

def submit_vote(voter_id: str, choice: str, signature: str, proof: dict):
    """Submit a vote"""
    response = requests.post(
        f"{API_URL}/vote",
        json={
            "voter_id": voter_id,
            "choice": choice,
            "signature": signature,
            "proof": proof
        }
    )
    return response.json()

def end_voting():
    """End the voting phase"""
    response = requests.post(f"{API_URL}/end-voting")
    return response.json()

def get_results():
    """Get election results"""
    response = requests.get(f"{API_URL}/results")
    return response.json()

def get_election_status():
    """Get current election status"""
    response = requests.get(f"{API_URL}/election-status")
    return response.json()

# Streamlit UI
st.title("S3Voting System")

# Sidebar for navigation
page = st.sidebar.selectbox(
    "Navigation",
    ["Setup Election", "Voter Registration", "Voting", "Results"]
)

# Get current election status
try:
    status = get_election_status()
except:
    status = {"is_active": False, "phase": "setup"}

if page == "Setup Election":
    st.header("Setup New Election")
    
    if not status["is_active"]:
        with st.form("election_setup"):
            election_name = st.text_input("Election Name")
            num_shards = st.number_input("Number of Shards", min_value=1, max_value=10, value=4)
            nodes_per_shard = st.number_input("Nodes per Shard", min_value=1, max_value=5, value=2)
            candidates = st.text_area("Candidates (one per line)")
            
            if st.form_submit_button("Setup Election"):
                if election_name and candidates:
                    try:
                        candidate_list = [c.strip() for c in candidates.split("\n") if c.strip()]
                        if not candidate_list:
                            st.error("Please enter at least one candidate")
                        else:
                            result = setup_election(num_shards, nodes_per_shard, election_name, candidate_list)
                            st.success(result["message"])  # Access message as dictionary key
                            st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Error setting up election: {str(e)}")
                else:
                    st.error("Please fill in all fields")
    else:
        st.info("An election is already active")

elif page == "Voter Registration":
    st.header("Voter Registration")
    
    if status["is_active"] and status["phase"] == "registration":
        with st.form("voter_registration"):
            voter_id = st.text_input("Voter ID")
            
            if st.form_submit_button("Register"):
                if voter_id :
                    result = register_voter(voter_id)
                    st.success(result["message"])
                else:
                    st.error("Please fill in all fields")
        
        if st.button("Start Voting Phase"):
            result = start_voting()
            st.success(result["message"])
            st.experimental_rerun()
    else:
        st.info("Registration phase is not active")

elif page == "Voting":
    st.header("Voting")
    
    if status["is_active"] and status["phase"] == "voting":
        with st.form("voting"):
            voter_id = st.text_input("Voter ID")
            choice = st.selectbox("Select Candidate", status["candidates"])
            
            if st.form_submit_button("Submit Vote"):
                if voter_id and choice:
                    # In a real system, this would be generated using proper cryptography
                    signature = "dummy_signature"
                    proof = {"dummy": 1}
                    
                    result = submit_vote(voter_id, choice, signature, proof)
                    st.success(result["message"])
                else:
                    st.error("Please fill in all fields")
        
        if st.button("End Voting"):
            result = end_voting()
            st.success(result["message"])
            st.experimental_rerun()
    else:
        st.info("Voting phase is not active")

elif page == "Results":
    st.header("Election Results")
    
    if status["is_active"] and status["phase"] == "results":
        try:
            results = get_results()
            if results["status"] == "success":
                # Display results in a bar chart
                st.bar_chart(results["results"])
                
                # Display detailed results
                st.subheader("Detailed Results")
                for candidate, votes in results["results"].items():
                    st.write(f"{candidate}: {votes} votes")
            else:
                st.error("Failed to fetch results")
        except:
            st.error("Failed to fetch results")
    else:
        st.info("Results are not yet available")

# Display current election status
st.sidebar.header("Election Status")
st.sidebar.write(f"Active: {status['is_active']}")
st.sidebar.write(f"Phase: {status['phase']}")
if status['is_active']:
    st.sidebar.write(f"Election: {status['election_name']}")
    st.sidebar.write(f"Registered Voters: {status['registered_voters']}")
    st.sidebar.write(f"Votes Cast: {status['votes_cast']}")
    st.sidebar.write(f"Number of Shards: {status['num_shards']}")
    st.sidebar.write(f"Total Nodes: {status['total_nodes']}") 