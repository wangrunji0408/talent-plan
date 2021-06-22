use futures::executor::block_on;
use std::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Duration,
};

use crate::proto::kvraftpb::*;

#[derive(Debug, Clone)]
enum ClientOp {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    leader: AtomicUsize,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Clerk").field(&self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            leader: AtomicUsize::new(0),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let arg = GetRequest { key: key.clone() };
        for i in self.leader.load(Ordering::Relaxed).. {
            let i = i % self.servers.len();
            match block_on(self.servers[i].get(&arg)) {
                Err(e) => panic!("{:?}", e),
                Ok(GetReply { wrong_leader, .. }) if wrong_leader => {
                    debug!("{:?} <-{} wrong leader", self, i);
                    if i == self.servers.len() - 1 {
                        thread::sleep(Duration::from_millis(100));
                    }
                    continue;
                }
                Ok(GetReply { value, .. }) => {
                    debug!("{:?} <-{} Get({:?})", self, i, key);
                    self.leader.store(i, Ordering::Relaxed);
                    return value;
                }
            }
        }
        todo!("retry")
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: ClientOp) {
        // You will have to modify this function.
        let arg = match op.clone() {
            ClientOp::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: Op::Append as _,
            },
            ClientOp::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: Op::Put as _,
            },
        };
        for i in self.leader.load(Ordering::Relaxed).. {
            let i = i % self.servers.len();
            match block_on(self.servers[i].put_append(&arg)) {
                Err(e) => panic!("{:?}", e),
                Ok(PutAppendReply { wrong_leader, .. }) if wrong_leader => {
                    debug!("{:?} <-{} wrong leader", self, i);
                    if i == self.servers.len() - 1 {
                        thread::sleep(Duration::from_millis(100));
                    }
                    continue;
                }
                Ok(PutAppendReply { .. }) => {
                    debug!("{:?} <-{} {:?}", self, i, op);
                    self.leader.store(i, Ordering::Relaxed);
                    return;
                }
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(ClientOp::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(ClientOp::Append(key, value))
    }
}
