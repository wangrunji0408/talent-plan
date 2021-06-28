pub mod raftpb {
    include!(concat!(env!("OUT_DIR"), "/raftpb.rs"));

    labrpc::service! {
        service raft {
            rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::raft::{
        add_service as add_raft_service, Client as RaftClient, Service as RaftService,
    };
}

pub mod kvraftpb {
    include!(concat!(env!("OUT_DIR"), "/kvraftpb.rs"));

    labrpc::service! {
        service kv {
            rpc get(GetRequest) returns (GetReply);
            rpc put_append(PutAppendRequest) returns (PutAppendReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::kv::{add_service as add_kv_service, Client as KvClient, Service as KvService};
}

pub mod shardkvpb {
    include!(concat!(env!("OUT_DIR"), "/shardkvpb.rs"));

    labrpc::service! {
        service shardkv {
            rpc get(GetRequest) returns (GetReply);
            rpc put_append(PutAppendRequest) returns (PutAppendReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::shardkv::{
        add_service as add_shardkv_service, Client as ShardKvClient, Service as ShardKvService,
    };
}

pub mod shardctrlerpb {
    include!(concat!(env!("OUT_DIR"), "/shardctrlerpb.rs"));

    labrpc::service! {
        service shardctrler {
            rpc join(JoinRequest) returns (JoinReply);
            rpc leave(LeaveRequest) returns (LeaveReply);
            rpc move_(MoveRequest) returns (MoveReply);
            rpc query(QueryRequest) returns (QueryReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::shardctrler::{
        add_service as add_shardctrler_service, Client as ShardCtrlerClient,
        Service as ShardCtrlerService,
    };
}
