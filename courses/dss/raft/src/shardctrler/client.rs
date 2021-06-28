use futures::executor::block_on;
use std::{fmt, thread, time::Duration};

use crate::proto::shardctrlerpb::*;

pub struct Clerk {
    pub name: String,
    pub servers: Vec<ShardCtrlerClient>,
    // Your data here.
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<ShardCtrlerClient>) -> Clerk {
        // You'll have to add code here.
        // Clerk { name, servers }
        crate::your_code_here((name, servers))
    }

    pub fn query(&self, num: Option<u64>) -> Config {
        // Your code here.
        let args = QueryRequest {
            num: num.unwrap_or(u64::MAX),
        };
        loop {
            // try each known server.
            for server in &self.servers {
                if let Ok(reply) = block_on(server.query(&args)) {
                    if !reply.wrong_leader {
                        return reply.config.unwrap();
                    }
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn join(&self, groups: Vec<Group>) {
        // Your code here.
        let args = JoinRequest { groups };
        loop {
            // try each known server.
            for server in &self.servers {
                if let Ok(reply) = block_on(server.join(&args)) {
                    if !reply.wrong_leader {
                        return;
                    }
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn leave(&self, gids: Vec<u64>) {
        // Your code here.
        let args = LeaveRequest { gids };
        loop {
            // try each known server.
            for server in &self.servers {
                if let Ok(reply) = block_on(server.leave(&args)) {
                    if !reply.wrong_leader {
                        return;
                    }
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn move_(&self, shard: u64, gid: u64) {
        // Your code here.
        let args = MoveRequest { shard, gid };
        loop {
            // try each known server.
            for server in &self.servers {
                if let Ok(reply) = block_on(server.move_(&args)) {
                    if !reply.wrong_leader {
                        return;
                    }
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}
