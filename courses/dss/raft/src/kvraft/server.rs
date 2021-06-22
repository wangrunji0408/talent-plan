use futures::{
    channel::{mpsc, oneshot},
    executor::ThreadPool,
    select, FutureExt, StreamExt,
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::proto::kvraftpb::*;
use crate::raft;

#[allow(dead_code)]
pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    max_raft_state: Option<usize>,
    // Your definitions here.
    runtime: ThreadPool,
    // { index -> (id, sender) }
    pending_rpcs: Arc<Mutex<HashMap<u64, (u64, oneshot::Sender<String>)>>>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Arc<dyn raft::persister::Persister>,
        max_raft_state: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, mut apply_ch) = mpsc::unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let rf = raft::Node::new(rf);

        let pending_rpcs = Arc::new(Mutex::new(
            HashMap::<u64, (u64, oneshot::Sender<String>)>::new(),
        ));
        let pending_rpcs0 = pending_rpcs.clone();
        let runtime = ThreadPool::new().unwrap();
        runtime.spawn_ok(async move {
            let mut ids = HashSet::new();
            let mut kv = HashMap::new();
            while let Some(msg) = apply_ch.next().await {
                match msg {
                    raft::ApplyMsg::Snapshot { .. } => todo!(),
                    raft::ApplyMsg::Command { index, data } => {
                        let cmd = labcodec::decode::<RaftCmd>(&data).unwrap();
                        let ret = match Op::from_i32(cmd.op).unwrap() {
                            Op::Put => {
                                // prevent duplicate put
                                if ids.insert(cmd.id) {
                                    kv.insert(cmd.key, cmd.value);
                                }
                                "".into()
                            }
                            Op::Append => {
                                // prevent duplicate append
                                if ids.insert(cmd.id) {
                                    kv.entry(cmd.key).or_default().push_str(&cmd.value);
                                }
                                "".into()
                            }
                            Op::Get => kv.get(&cmd.key).cloned().unwrap_or_default(),
                            Op::Unknown => unreachable!(),
                        };
                        let mut pending_rpcs = pending_rpcs0.lock().unwrap();
                        if let Some((id, sender)) = pending_rpcs.remove(&index) {
                            if cmd.id == id {
                                // message match, success
                                let _ = sender.send(ret);
                            }
                            // otherwise drop the sender
                        }
                    }
                }
            }
        });

        KvServer {
            rf,
            me,
            max_raft_state,
            runtime,
            pending_rpcs,
        }
    }

    fn register_rpc(&self, index: u64, id: u64) -> oneshot::Receiver<String> {
        let (sender, recver) = oneshot::channel();
        self.pending_rpcs
            .lock()
            .unwrap()
            .insert(index, (id, sender));
        recver
    }
}

impl fmt::Debug for KvServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KvServer({})", self.me,)
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
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
    server: Arc<KvServer>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        Node {
            server: Arc::new(kv),
        }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
        self.server.rf.kill();
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.server.rf.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.
        let cmd = RaftCmd {
            key: arg.key.clone(),
            value: "".into(),
            op: Op::Get as _,
            id: rand::random(),
        };
        let index = match self.server.rf.start(&cmd) {
            Ok((index, _)) => index,
            Err(raft::errors::Error::NotLeader) => {
                return Ok(GetReply {
                    wrong_leader: true,
                    err: "".into(),
                    value: "".into(),
                })
            }
            e => panic!("{:?}", e),
        };
        let recver = self.server.register_rpc(index, cmd.id);
        debug!("{:?} start {:?}", self.server, arg);
        match timeout(recver).await {
            None => {
                debug!("{:?} Tout  {:?}", self.server, arg);
                Ok(GetReply {
                    wrong_leader: false,
                    err: "commit timeout".into(),
                    value: "".into(),
                })
            }
            Some(Err(_)) => {
                debug!("{:?} fail  {:?}", self.server, arg);
                Ok(GetReply {
                    wrong_leader: false,
                    err: "commit failed".into(),
                    value: "".into(),
                })
            }
            Some(Ok(value)) => {
                debug!("{:?} ok    {:?}", self.server, arg);
                Ok(GetReply {
                    wrong_leader: false,
                    err: "".into(),
                    value,
                })
            }
        }
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        let cmd = RaftCmd {
            key: arg.key.clone(),
            value: arg.value.clone(),
            op: arg.op.clone(),
            id: arg.id,
        };
        let index = match self.server.rf.start(&cmd) {
            Ok((index, _)) => index,
            Err(raft::errors::Error::NotLeader) => {
                return Ok(PutAppendReply {
                    wrong_leader: true,
                    err: "".into(),
                })
            }
            e => panic!("{:?}", e),
        };
        let recver = self.server.register_rpc(index, cmd.id);
        debug!("{:?} start {:?}", self.server, arg);
        match timeout(recver).await {
            None => {
                debug!("{:?} Tout  {:?}", self.server, arg);
                Ok(PutAppendReply {
                    wrong_leader: false,
                    err: "commit timeout".into(),
                })
            }
            Some(Err(_)) => {
                debug!("{:?} fail  {:?}", self.server, arg);
                Ok(PutAppendReply {
                    wrong_leader: false,
                    err: "commit failed".into(),
                })
            }
            Some(Ok(_)) => {
                debug!("{:?} ok    {:?}", self.server, arg);
                Ok(PutAppendReply {
                    wrong_leader: false,
                    err: "".into(),
                })
            }
        }
    }
}

async fn timeout<F: Future>(f: F) -> Option<F::Output> {
    select! {
        ret = f.fuse() => Some(ret),
        _ = futures_timer::Delay::new(Duration::from_millis(500)).fuse() => None,
    }
}
