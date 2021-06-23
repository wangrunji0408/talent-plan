use futures::{
    channel::{mpsc, oneshot},
    executor::ThreadPool,
    select, FutureExt, StreamExt,
};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::proto::kvraftpb::*;
use crate::raft;

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // Your definitions here.
    #[allow(dead_code)]
    runtime: ThreadPool,
    // { index -> (id, sender) }
    pending_puts: Arc<Mutex<HashMap<u64, (u64, oneshot::Sender<()>)>>>,
    pending_gets: Arc<Mutex<HashMap<u64, (String, oneshot::Sender<String>)>>>,
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
        let rf = raft::Raft::new(servers, me, persister.clone(), tx);
        let rf = raft::Node::new(rf);

        let pending_puts = Arc::new(Mutex::new(HashMap::<u64, (u64, oneshot::Sender<()>)>::new()));
        let pending_gets = Arc::new(Mutex::new(
            HashMap::<u64, (String, oneshot::Sender<String>)>::new(),
        ));
        let pending_puts0 = pending_puts.clone();
        let pending_gets0 = pending_gets.clone();
        let rf0 = rf.clone();
        let runtime = ThreadPool::new().unwrap();
        runtime.spawn_ok(async move {
            let mut state = State::default();
            let mut state_index = 0;
            while let Some(msg) = apply_ch.next().await {
                match msg {
                    raft::ApplyMsg::Snapshot { index, data, .. } => {
                        state = State::from_snapshot(&data);
                        state_index = index;
                    }
                    raft::ApplyMsg::Read { id } => {
                        // ACK to gets
                        let mut pending_gets = pending_gets0.lock().unwrap();
                        if let Some((key, sender)) = pending_gets.remove(&id) {
                            let value = state.get(&key);
                            let _ = sender.send(value);
                        }
                    }
                    raft::ApplyMsg::Command { index, data } => {
                        let cmd = labcodec::decode::<RaftCmd>(&data).unwrap();
                        let cmd_id = cmd.id;
                        state.apply(cmd);
                        state_index = index;

                        // send result to RPC
                        let mut pending_puts = pending_puts0.lock().unwrap();
                        if let Some((id, sender)) = pending_puts.remove(&index) {
                            if cmd_id == id {
                                // message match, success
                                let _ = sender.send(());
                            }
                            // otherwise drop the sender
                        }
                    }
                }
                // snapshot if needed
                if let Some(size) = max_raft_state {
                    if state_index % 10 == 0 && persister.raft_state().len() >= size {
                        rf0.snapshot(state_index, &state.snapshot());
                    }
                }
            }
        });

        KvServer {
            rf,
            me,
            runtime,
            pending_puts,
            pending_gets,
        }
    }

    fn register_put(&self, index: u64, id: u64) -> oneshot::Receiver<()> {
        let (sender, recver) = oneshot::channel();
        self.pending_puts
            .lock()
            .unwrap()
            .insert(index, (id, sender));
        recver
    }

    fn register_get(&self, id: u64, key: String) -> oneshot::Receiver<String> {
        let (sender, recver) = oneshot::channel();
        self.pending_gets.lock().unwrap().insert(id, (key, sender));
        recver
    }
}

impl fmt::Debug for KvServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KvServer({})", self.me,)
    }
}

#[derive(Debug, Default)]
struct State {
    kv: HashMap<String, String>,
    // A circular queue with max capacity 50
    ids: Vec<u64>,
}

impl State {
    fn snapshot(&self) -> Vec<u8> {
        let mut keys = vec![];
        let mut values = vec![];
        keys.reserve(self.kv.len());
        values.reserve(self.kv.len());
        for (k, v) in self.kv.iter() {
            keys.push(k.clone());
            values.push(v.clone());
        }
        let snapshot = RaftSnapshot {
            keys,
            values,
            ids: self.ids.clone(),
        };
        let mut data = vec![];
        labcodec::encode(&snapshot, &mut data).unwrap();
        data
    }

    fn from_snapshot(data: &[u8]) -> Self {
        let s = labcodec::decode::<RaftSnapshot>(data).unwrap();
        let keys = s.keys.iter().cloned();
        let values = s.values.iter().cloned();
        State {
            kv: keys.zip(values).collect(),
            ids: s.ids,
        }
    }

    fn apply(&mut self, cmd: RaftCmd) {
        let unique = !self.ids.contains(&cmd.id);
        if self.ids.len() > 50 {
            self.ids.remove(0);
        }
        self.ids.push(cmd.id);
        match Op::from_i32(cmd.op).unwrap() {
            Op::Put => {
                if unique {
                    self.kv.insert(cmd.key, cmd.value);
                }
            }
            Op::Append => {
                if unique {
                    self.kv.entry(cmd.key).or_default().push_str(&cmd.value);
                }
            }
            _ => unreachable!(),
        }
    }

    fn get(&self, key: &str) -> String {
        self.kv.get(key).cloned().unwrap_or_default()
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
        let id = rand::random();
        match self.server.rf.start_read(id) {
            Ok(_) => {}
            Err(raft::errors::Error::NotLeader) => {
                return Ok(GetReply {
                    wrong_leader: true,
                    err: "".into(),
                    value: "".into(),
                })
            }
            e => panic!("{:?}", e),
        };
        let recver = self.server.register_get(id, arg.key.clone());
        debug!("{:?} start {:?}", self.server, arg);
        match timeout(recver).await {
            None => {
                debug!("{:?} Tout  {:?}", self.server, arg);
                Ok(GetReply {
                    wrong_leader: false,
                    err: "get timeout".into(),
                    value: "".into(),
                })
            }
            Some(Err(_)) => {
                debug!("{:?} fail  {:?}", self.server, arg);
                Ok(GetReply {
                    wrong_leader: false,
                    err: "get failed".into(),
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
        let recver = self.server.register_put(index, cmd.id);
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
