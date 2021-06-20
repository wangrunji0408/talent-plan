use futures::{
    channel::mpsc::UnboundedSender, executor::ThreadPool, prelude::*, select,
    stream::FuturesUnordered,
};
use std::{
    fmt,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::raft::append_entries_args::Log;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct State {
    term: u64,
    role: Role,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    apply_ch: UnboundedSender<ApplyMsg>,
    this: Weak<Mutex<Self>>,
    runtime: ThreadPool,

    state: State,
    // State Persistent state on all servers
    voted_for: Option<usize>,
    log: Vec<Log>,
    // Volatile state on all servers
    commit_index: usize,
    last_applied: usize,
    // Volatile state on leaders
    next_index: Vec<usize>,
    match_index: Vec<usize>,

    last_apply_entries_received: Instant,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Arc<Mutex<Raft>> {
        let raft_state = persister.raft_state();
        let n = peers.len();

        // Your initialization code here (2A, 2B, 2C).
        let rf = Arc::new(Mutex::new(Raft {
            peers,
            persister,
            me,
            apply_ch,
            this: Weak::default(),
            runtime: ThreadPool::new().unwrap(),
            state: State::default(),
            voted_for: None,
            log: vec![Log::default()],
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; n],
            match_index: vec![0; n],
            last_apply_entries_received: Instant::now(),
        }));

        let mut raft = rf.lock().unwrap();
        raft.this = Arc::downgrade(&rf);
        raft.spawn_follower_timer();

        // initialize from state persisted before a crash
        raft.restore(&raft_state);
        info!("{:?} created", raft);
        drop(raft);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    fn start(&mut self, command: &impl labcodec::Message) -> Result<(u64, u64)> {
        let mut data = vec![];
        labcodec::encode(command, &mut data).map_err(Error::Encode)?;
        // Your code here (2B).
        if !self.state.is_leader() {
            return Err(Error::NotLeader);
        }
        let index = self.log.len() as u64;
        let term = self.state.term;
        self.log.push(Log { term, data });
        info!("{:?} start index {}", self, index);
        Ok((index, term))
    }

    fn transfer_state(&mut self, term: u64, role: Role, reason: &str) {
        assert!(term >= self.state.term);
        info!("{:?} {} => t={},{:?}", self, reason, term, role);
        if term > self.state.term {
            self.voted_for = None;
        }
        self.state = State { term, role };
        match role {
            Role::Follower => {
                self.last_apply_entries_received = Instant::now();
                self.spawn_follower_timer();
            }
            Role::Candidate => {
                self.voted_for = Some(self.me);
                self.spawn_candidate_task();
            }
            Role::Leader => {
                self.next_index.fill(self.log.len());
                self.match_index.fill(0);
                self.spawn_leader_append_entries();
            }
        }
    }

    fn spawn_follower_timer(&self) {
        let this = self.this.clone();
        let state = self.state;
        let timeout = Self::generate_election_timeout();
        self.runtime.spawn_ok(async move {
            while let Some(this) = this.upgrade() {
                let deadline = {
                    let mut this = this.lock().unwrap();
                    if this.state != state {
                        return;
                    }
                    if this.last_apply_entries_received.elapsed() > timeout {
                        this.transfer_state(state.term + 1, Role::Candidate, "election timeout");
                        return;
                    }
                    this.last_apply_entries_received + timeout
                };
                sleep_until(deadline).await;
            }
        });
    }

    fn spawn_candidate_task(&self) {
        let this = self.this.clone();
        let state = self.state;
        let args = RequestVoteArgs {
            term: self.state.term,
            candidate_id: self.me as u64,
            last_log_index: self.log.len() as u64 - 1,
            last_log_term: self.log.last().unwrap().term,
        };
        debug!("{:?} -> {:?}", self, args);
        let mut rpcs = self
            .peers
            .iter()
            .enumerate()
            .filter(|&(idx, _)| idx != self.me)
            .map(|(_, peer)| peer.clone().request_vote(&args))
            .collect::<FuturesUnordered<_>>();
        let deadline = Instant::now() + Self::generate_election_timeout();
        let min_vote = (self.peers.len() + 1) / 2;
        self.runtime.spawn_ok(async move {
            let mut vote_count = 1;
            loop {
                #[derive(Debug)]
                enum Event {
                    Reply(RequestVoteReply),
                    RpcError,
                    AllComplete,
                    Timeout,
                }
                let event = select! {
                    _ = sleep_until(deadline).fuse() => Event::Timeout,
                    ret = rpcs.next() => match ret {
                        None => Event::AllComplete,
                        Some(Ok(reply)) => Event::Reply(reply),
                        Some(Err(_)) => Event::RpcError,
                    }
                };
                let this = match this.upgrade() {
                    Some(t) => t,
                    None => return,
                };
                let mut this = this.lock().unwrap();
                if this.state != state {
                    return;
                }
                match event {
                    Event::Reply(reply) => {
                        assert!(reply.term >= state.term);
                        if reply.term > state.term {
                            this.transfer_state(reply.term, Role::Follower, "higher term (->RV)");
                            return;
                        }
                        if reply.vote_granted {
                            vote_count += 1;
                            if vote_count >= min_vote {
                                this.transfer_state(state.term, Role::Leader, "win election");
                                return;
                            }
                        }
                    }
                    Event::RpcError => {}
                    Event::Timeout => {
                        this.transfer_state(state.term + 1, Role::Candidate, "election timeout");
                        return;
                    }
                    Event::AllComplete => {}
                }
            }
        });
    }

    fn spawn_leader_append_entries(&self) {
        for (i, peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            let weak = self.this.clone();
            let state = self.state;
            let leader_id = self.me as u64;
            let peer = peer.clone();
            self.runtime.spawn_ok(async move {
                let mut backoff = 1;
                while let Some(this) = weak.upgrade() {
                    let (match_index_if_success, args) = {
                        let this = this.lock().unwrap();
                        if this.state != state {
                            return;
                        }
                        let next_index = this.next_index[i];
                        let prev_log_index = next_index - 1;
                        let args = AppendEntriesArgs {
                            term: state.term,
                            leader_id,
                            prev_log_index: prev_log_index as u64,
                            prev_log_term: this.log[prev_log_index].term,
                            entries: this.log[next_index..].into(),
                            leader_commit: this.commit_index as u64,
                        };
                        debug!("{:?} ->{} {:?}", *this, i, args);
                        (prev_log_index + args.entries.len(), args)
                    };
                    let reply = select! {
                        ret = peer.append_entries(&args).fuse() => match ret {
                            Err(_) => continue,
                            Ok(x) => x,
                        },
                        _ = futures_timer::Delay::new(Self::generate_heartbeat_interval()).fuse() => continue,
                    };
                    {
                        let mut this = this.lock().unwrap();
                        if reply.term > this.state.term {
                            this.transfer_state(reply.term, Role::Follower, "higher term (->AE)");
                        }
                        if this.state != state {
                            return;
                        }
                        if reply.success {
                            this.next_index[i] = match_index_if_success + 1;
                            this.match_index[i] = match_index_if_success;
                            this.update_commit_from_match();
                        } else {
                            if backoff >= this.next_index[i] {
                                this.next_index[i] = 1;
                            } else {
                                this.next_index[i] -= backoff;
                            }
                            backoff *= 2;
                            continue;
                        }
                    }
                    futures_timer::Delay::new(Self::generate_heartbeat_interval()).await;
                }
            });
        }
    }

    fn update_commit_from_match(&mut self) {
        assert!(self.state.is_leader());
        let mut match_index = self.match_index.clone();
        match_index.sort();
        let majority = self.peers.len() - self.peers.len() / 2;
        let commit_index = match_index[majority];
        if self.log[commit_index].term != self.state.term {
            return;
        }
        self.update_commit(commit_index);
    }

    fn update_commit(&mut self, commit_index: usize) {
        for i in self.commit_index + 1..=commit_index {
            self.apply_ch
                .unbounded_send(ApplyMsg {
                    command_valid: true,
                    command: self.log[i].data.clone(),
                    command_index: i as u64,
                })
                .unwrap();
        }
        if commit_index > self.commit_index {
            self.commit_index = commit_index;
            info!("{:?} commit to index {}", self, commit_index);
        }
    }

    fn generate_election_timeout() -> Duration {
        Duration::from_millis(150 + rand::random::<u64>() % 150)
    }

    fn generate_heartbeat_interval() -> Duration {
        Duration::from_millis(50)
    }
}

async fn sleep_until(deadline: Instant) {
    futures_timer::Delay::new(deadline.duration_since(Instant::now())).await;
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Raft({},t={},l={},{:?})",
            self.me,
            self.state.term,
            self.log.len() - 1,
            self.state.role
        )
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Arc<Mutex<Raft>>) -> Node {
        // Your code here.
        Node { raft }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start(&self, command: &impl labcodec::Message) -> Result<(u64, u64)> {
        // Your code here.
        let mut raft = self.raft.lock().unwrap();
        raft.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        let raft = self.raft.lock().unwrap();
        raft.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        let raft = self.raft.lock().unwrap();
        raft.state.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let raft = self.raft.lock().unwrap();
        raft.state.clone()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut this = self.raft.lock().unwrap();
        // Reply false if term < currentTerm (§5.1)
        if args.term < this.state.term {
            debug!("{:?} <- deny {:?}", *this, args);
            return Ok(RequestVoteReply {
                term: this.state.term,
                vote_granted: false,
            });
        }
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if args.term > this.state.term {
            this.transfer_state(args.term, Role::Follower, "higher term (<-RV)");
        }
        // If votedFor is null or candidateId, and candidate’s log is at least
        // as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        let can_vote =
            this.voted_for.is_none() || this.voted_for == Some(args.candidate_id as usize);
        let log_up_to_date = (args.last_log_term, args.last_log_index)
            >= (this.log.last().unwrap().term, this.log.len() as u64 - 1);
        let vote_granted = can_vote && log_up_to_date;
        if vote_granted {
            this.voted_for = Some(args.candidate_id as usize);
        }
        debug!(
            "{:?} <- {} {:?}",
            *this,
            if vote_granted { "accept" } else { "deny" },
            args
        );
        Ok(RequestVoteReply {
            term: this.state.term,
            vote_granted,
        })
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut this = self.raft.lock().unwrap();
        // Reply false if term < currentTerm (§5.1)
        if args.term < this.state.term {
            debug!("{:?} <- deny {:?}", *this, args);
            return Ok(AppendEntriesReply {
                term: this.state.term,
                success: false,
            });
        }
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if args.term > this.state.term {
            this.transfer_state(args.term, Role::Follower, "higher term (<-AE)");
        }
        this.last_apply_entries_received = Instant::now();
        // Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)
        if !matches!(this.log.get(args.prev_log_index as usize), Some(log) if log.term == args.prev_log_term)
        {
            debug!("{:?} <- deny {:?}", *this, args);
            return Ok(AppendEntriesReply {
                term: this.state.term,
                success: false,
            });
        }
        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it (§5.3)
        let log_offset = args.prev_log_index as usize + 1;
        if let Some(idx) = args
            .entries
            .iter()
            .zip(this.log[log_offset..].iter())
            .position(|(remote, local)| remote.term != local.term)
        {
            this.log.truncate(log_offset + idx);
        }
        // Append any new entries not already in the log
        if log_offset + args.entries.len() > this.log.len() {
            let new_logs = &args.entries[this.log.len() - log_offset..];
            this.log.extend_from_slice(new_logs);
        }
        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if args.leader_commit as usize > this.commit_index {
            let commit_index = (args.leader_commit as usize).min(this.log.len() - 1);
            this.update_commit(commit_index);
        }
        debug!("{:?} <- accept {:?}", *this, args);
        Ok(AppendEntriesReply {
            term: this.state.term,
            success: true,
        })
    }
}
