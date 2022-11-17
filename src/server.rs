use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};
use log::info;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq)]
pub enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Heartbeat {
    pub term: u64,
    pub peer_id: String,
}

pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
}

#[derive(Debug)]
pub struct Peer {
    pub id: String,
    pub address: SocketAddrV4,
}

#[derive(Debug)]
pub struct Leader {
    pub id: String,
    pub term: u64,
}

#[derive(Debug)]
pub struct Server {
    pub id: String,
    pub address: SocketAddrV4,
    pub state: State,
    pub term: u64,
    pub log_entries: Vec<Heartbeat>,
    pub voted_for: Option<Peer>,
    pub next_timeout: Option<Instant>,
    pub timeout: Duration,
    pub current_leader: Option<Leader>,
    pub number_of_peers: usize,
}

impl Server {
    pub fn new(timeout: Duration, number_of_peers: usize, address: SocketAddrV4, id: String) -> Server {
        Server {
            id,
            address,
            state: State::FOLLOWER,
            term: 0,
            log_entries: Vec::new(),
            voted_for: None,
            next_timeout: None,
            timeout,
            current_leader: None,
            number_of_peers,
        }
    }

    pub fn reset_timeout(&mut self) {
        self.next_timeout = Some(Instant::now() + self.timeout);
    }

    pub fn become_leader(self: &mut Self) {
        if self.state == State::CANDIDATE {
            info!(
                "Server {} has won the election! The new term is: {}",
                self.id, self.term
            );
            self.state = State::LEADER;
            self.next_timeout = None;
        }
    }

    pub fn check_timeout(self: &mut Self) -> bool {
        match self.next_timeout {
            Some(t) => Instant::now() > t,
            None => false,
        }
    }
}