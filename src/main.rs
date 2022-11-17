pub mod rpc;
pub mod server;
pub mod raft;


use std::sync::Arc;
use std::sync::Mutex;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::thread;
use log::info;
use rand::Rng;
use std::time::{Duration, Instant};

use crate::rpc::RpcServer;
use crate::rpc::RpcClient;
use crate::server::{Server, Peer};
use log::LevelFilter;
use simplelog::{Config, TermLogger, TerminalMode};

pub struct Cluster {
    n : usize,

    host: Ipv4Addr,

    ports: Vec<u16>,

}

impl Cluster {
    fn new(n: usize) -> Cluster {
        let mut ports = Vec::new();
        for i in 0..n {
            ports.push(3300 + i as u16);
        }
        Cluster {
            n,
            host: Ipv4Addr::LOCALHOST,
            ports,
        }
    }

    fn get_address(&self, i: usize) -> SocketAddrV4 {
        SocketAddrV4::new(self.host, self.ports[i])
    }

    fn get_server(&self, i: usize) ->  Arc<Mutex<Server>> {
        let mut rng = rand::thread_rng();
        let address = self.get_address(i);
        let d: u64;
        if i==0 {
            d = 4;
        } else if i==1 {
            d = 5;
        } else {
            d = 5;
        }
        let server = Arc::new(Mutex::new(Server::new(
            Duration::new(d, 0),
            self.n-1,
            address,
            format!("server_{}", i+1),
        )));
        server
    }

    fn get_peers(&self, i:usize) -> Vec<Peer> {
        let mut peers = Vec::new();
        for j in 0..self.n {
            if i != j {
                peers.push(Peer {
                    id: format!("server_{}", j+1),
                    address: self.get_address(j),
                });
            }
        }
        peers
    }

    fn run(&self) {
        let mut rng = rand::thread_rng();
        let mut rpc_servers = Vec::new();
        let mut threads = Vec::new();
        let mut servers = Vec::new();
        for i in 0..self.n {
            let server = self.get_server(i);
            servers.push(server);
            rpc_servers.push(RpcServer::new(Arc::clone(&servers[i]), self.get_address(i)));
        }

        for rpc_server in rpc_servers {
            let thread = thread::spawn(move || {
                rpc_server.run();
            });
            threads.push(thread);
        }

        thread::sleep(Duration::new(1, 0));
        let mut raft_threads = Vec::new();
        for (i, server) in servers.into_iter().enumerate() {
            let peers = self.get_peers(i);
            let rpc_id = server.lock().unwrap().id.clone();
            let timeout = server.lock().unwrap().timeout.as_secs();
            raft_threads.push(thread::spawn(move || {
                let client = RpcClient::new(&peers);
                {
                    info!(
                        "[{}] {} has a timeout of {} seconds",
                        i,
                        rpc_id,
                        timeout
                    )
                }
                crate::raft::start_server(Arc::clone(&server), client);
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        for thread in raft_threads {
            thread.join().unwrap();
        }
    }
}


fn main() {
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed).unwrap();
    let cluster = Cluster::new(3);
    cluster.run();
}
