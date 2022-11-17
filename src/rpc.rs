use std::sync::Arc;
use std::sync::Mutex;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::thread;
use log::info;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::collections::HashMap;

use crate::server::{Server, Peer, Heartbeat, State, Leader, VoteResponse, VoteRequest};


#[derive(Serialize, Deserialize, Debug)]
pub enum RpcMessage {
    VoteRequest { term: u64, candidate_id: String },
    VoteResponse { term: u64, vote_granted: bool },
    Heartbeat { term: u64, peer_id: String },
    HeartbeatResponse { term: u64, peer_id: String },
}

#[derive(Debug)]
pub struct RpcServer {
    pub server: Arc<Mutex<Server>>,
    pub address: SocketAddrV4,
}


impl RpcServer {
    pub fn new(server: Arc<Mutex<Server>>, address: SocketAddrV4) -> Self {
        Self {
            server,
            address,
        }
    }

    pub fn run(&self) {
        info!("Starting RPC server on {}", self.address);
        let listener = TcpListener::bind(self.address).unwrap();
        for stream in listener.incoming() {
            let s_clone = Arc::clone(&self.server);
            match stream {
                Ok(stream) => {
                    thread::spawn(move || handle_connection(s_clone, stream));
                }
                Err(e) => {
                    info!("Error: {}", e);
                }
            }
        }
    }
}
    fn handle_connection(server: Arc<Mutex<Server>>, mut stream: TcpStream) {
        loop {
            let mut buffer = [0; 256];
            stream.read(&mut buffer).unwrap();
    
            let deserialized: RpcMessage = bincode::deserialize(&buffer).unwrap();
    
            let response = match deserialized {
                RpcMessage::Heartbeat { term, peer_id } => {
                    handle_log_entry(Arc::clone(&server), term, peer_id)
                }
                RpcMessage::VoteRequest { term, candidate_id } => {
                    handle_vote_request(Arc::clone(&server), term, candidate_id)
                }
                _ => Vec::new(),
            };
    
            stream.write(&response).unwrap();
            stream.flush().unwrap();
        }
    }

    fn handle_log_entry(server: Arc<Mutex<Server>>, term: u64, peer_id: String) -> Vec<u8> {
        let mut server = server.lock().unwrap();
        let heartbeat = Heartbeat { term, peer_id: peer_id.clone() };

        if let Heartbeat { term, peer_id } = heartbeat {
            info! (
                "[{}] Term {} Received heartbeat from {} with term {}", 
                server.id.split("_").collect::<Vec<&str>>()[1], 
                server.term, 
                peer_id, 
                term
            );
            server.reset_timeout();
            if term > server.term {
                info!(
                    "Server {} becoming follower. The new leader is: {}",
                    server.id, peer_id
                );    
                server.term = term;
                server.state = State::FOLLOWER;
                server.current_leader = None;
                server.current_leader = Some(Leader { id: peer_id, term });
            }
        };

    
        let response = RpcMessage::HeartbeatResponse {
            term: server.term,
            peer_id: peer_id.clone()
        };
    
        bincode::serialize(&response).unwrap()
    }
    
    fn handle_vote_request(server: Arc<Mutex<Server>>, term: u64, candidate_id: String) -> Vec<u8> {
        let mut t_server = server.lock().unwrap();
        let mut response : VoteResponse;
        match t_server.voted_for {
            Some(_) => {
                response = VoteResponse {
                    term: t_server.term,
                    vote_granted: false,
                };
            },
            None => {
                if term > t_server.term {
                    t_server.voted_for = Some(
                        Peer {
                            id: candidate_id,
                            address: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7878),
                        }
                    );
                    response = VoteResponse {
                        term: t_server.term,
                        vote_granted: true,
                    };
                } else {
                    response = VoteResponse {
                        term: t_server.term,
                        vote_granted: false,
                    };
                }
            }
        }
    
        let response = RpcMessage::VoteResponse {
            term: response.term,
            vote_granted: response.vote_granted,
        };
    
        bincode::serialize(&response).unwrap()
    }
    

pub struct RpcClient {
    pub peer_servers: HashMap<String, TcpStream>,
}

impl RpcClient {
    pub fn new(peers: &Vec<Peer>) -> Self {
        let mut peer_servers = HashMap::new();
        for peer in peers {
            let stream = TcpStream::connect(&peer.address).unwrap();
            peer_servers.insert(peer.id.to_string(), stream);
        }
        Self { peer_servers }
    }

    pub fn send_vote_request(&self, request: VoteRequest) -> Vec<VoteResponse> {
        let rpc_message = RpcMessage::VoteRequest {
            term: request.term,
            candidate_id: request.candidate_id,
        };

        let request_vote_bin = bincode::serialize(&rpc_message).unwrap();
        let mut rpc_responses: Vec<RpcMessage> = Vec::new();

        for mut stream in self.peer_servers.values() {
            stream.write(&request_vote_bin).unwrap();

            let mut buffer = [0; 256];
            stream.read(&mut buffer).unwrap();
            rpc_responses.push(bincode::deserialize(&buffer).unwrap());
        }

        let mut response = Vec::new();
        for rpc_resp in rpc_responses {
            if let RpcMessage::VoteResponse { term, vote_granted } = rpc_resp {
                response.push(VoteResponse {
                    term: term,
                    vote_granted: vote_granted,
                });
            }
        }

        response
    }

    pub fn send_heartbeat(&self, heartbeat: Heartbeat) {
        if let Heartbeat { term, peer_id } = heartbeat {
            let rpc_message = RpcMessage::Heartbeat {
                term: term,
                peer_id: peer_id,
            };

            let broadcast_bin = bincode::serialize(&rpc_message).unwrap();

            for mut stream in self.peer_servers.values() {
                stream.write(&broadcast_bin).unwrap();

                let mut buffer = [0; 256];
                stream.read(&mut buffer).unwrap();
            }
        }
    }
}
