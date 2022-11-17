use std::thread;
use std::sync::{Arc, Mutex};
use log::info;
use math::round;
use rand::Rng;
use std::time::Duration;
use crate::server::{Server, Peer, VoteResponse, VoteRequest, Heartbeat, State};
use crate::rpc::{RpcServer, RpcClient};

pub fn start_server (
    server: Arc<Mutex<Server>>,
    client: RpcClient,
) {
    server.lock().unwrap().reset_timeout();
    let raft_task = thread::spawn(move || {
        loop {
            let timed_out: bool = server.lock().unwrap().check_timeout();
            if timed_out {
                info!("{} timed out", server.lock().unwrap().id);
                elect_leader(Arc::clone(&server), &client);
            }
            let is_leader = server.lock().unwrap().state == State::LEADER;
            if is_leader {
                let term = server.lock().unwrap().term;
                let peer_id = server.lock().unwrap().id.clone();
                let heartbeat = Heartbeat {
                    term: term,
                    peer_id: peer_id,
                };
                client.send_heartbeat(heartbeat);
                let mut rng = rand::thread_rng();
                thread::sleep(Duration::new(rng.gen_range(1..7), 0));
            }
        }
    });

    raft_task.join().unwrap();
}


fn elect_leader(
    server: Arc<Mutex<Server>>,
    client: &RpcClient,
) {
    let vote_request  = prepare_vote_request(Arc::clone(&server));
    let id = server.lock().unwrap().id.clone();
    let term = server.lock().unwrap().term;

    info!("{} has started election with term {}", id, term);

    let vote_response = match vote_request {
        Some(vote_request) => Some(client.send_vote_request(vote_request)),
        None => None,
    };

    if let Some(res) = vote_response {
        let self_election;
        {
            let mut server_tmp = server.lock().unwrap();
            self_election = check_if_leader(res, &server_tmp) && !server_tmp.check_timeout();
        }

        if self_election {
            let mut server_tmp = server.lock().unwrap();
            server_tmp.become_leader();
            let term = server_tmp.term;
            let id = server_tmp.id.clone();
            client.send_heartbeat(Heartbeat {
                term: term,
                peer_id: id,
            });
        }
    }
}

fn check_if_leader(response: Vec<VoteResponse>, server: &Server) -> bool {
    let number_of_servers = server.number_of_peers + 1; 

    let votes = response.iter().filter(|r| r.vote_granted).count();

    let min_quorum = round::floor((number_of_servers / 2) as f64, 0);

    (votes + 1) > min_quorum as usize && State::CANDIDATE == server.state
}

fn prepare_vote_request(server: Arc<Mutex<Server>>) -> Option<VoteRequest> {
    if server.lock().unwrap().state == State::LEADER {
        return None;
    }

    {
        let mut server_tmp = server.lock().unwrap();
        server_tmp.state = State::CANDIDATE;
        server_tmp.term = server_tmp.term + 1;
        server_tmp.reset_timeout();
        server_tmp.voted_for = Some(Peer {
            id: server_tmp.id.to_string(),
            address: server_tmp.address,
        });
    }

    let new_term = server.lock().unwrap().term;
    let id = server.lock().unwrap().id.to_string();

    Some(VoteRequest {
        term: new_term,
        candidate_id: id,
    })
}