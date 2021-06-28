use futures::channel::mpsc;
use std::sync::Arc;

use crate::proto::shardctrlerpb::*;
use crate::raft;

pub struct ShardCtrler {
    pub rf: raft::Node,
    me: usize,
    apply_ch: mpsc::UnboundedSender<raft::ApplyMsg>,
    configs: Vec<Config>,
    // Your definitions here.
}

impl ShardCtrler {
    // servers[] contains the ports of the set of
    // servers that will cooperate via Raft to
    // form the fault-tolerant shardctrler service.
    // me is the index of the current server in servers[].
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Arc<dyn raft::persister::Persister>,
    ) -> ShardCtrler {
        // You may need initialization code here.

        let (tx, apply_ch) = mpsc::unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        crate::your_code_here((rf, apply_ch))
    }
}

impl ShardCtrler {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.apply_ch;
        let _ = &self.configs;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<ShardCtrler>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
}

impl Node {
    pub fn new(kv: ShardCtrler) -> Node {
        // Your code here.
        crate::your_code_here(kv);
    }

    // the tester calls Kill() when a ShardCtrler instance won't
    // be needed again. you are not required to do anything
    // in Kill(), but it might be convenient to (for example)
    // turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call shardctrler::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        raft::State {
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl ShardCtrlerService for Node {
    async fn join(&self, arg: JoinRequest) -> labrpc::Result<JoinReply> {
        // Your code here.
        crate::your_code_here(arg)
    }

    async fn leave(&self, arg: LeaveRequest) -> labrpc::Result<LeaveReply> {
        // Your code here.
        crate::your_code_here(arg)
    }

    async fn move_(&self, arg: MoveRequest) -> labrpc::Result<MoveReply> {
        // Your code here.
        crate::your_code_here(arg)
    }

    async fn query(&self, arg: QueryRequest) -> labrpc::Result<QueryReply> {
        // Your code here.
        crate::your_code_here(arg)
    }
}
