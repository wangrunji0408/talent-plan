//! Support for Raft and kvraft to save persistent
//! Raft state (log &c) and k/v server snapshots.
//!
//! we will use the original persister.rs to test your code for grading.
//! so, while you can modify this code to help you debug, please
//! test with the original before submitting.

use std::sync::Mutex;

pub trait Persister: Send + 'static {
    fn raft_state(&self) -> Vec<u8>;
    fn save_raft_state(&self, state: Vec<u8>);
    fn save_state_and_snapshot(&self, state: Vec<u8>, snapshot: Vec<u8>);
    fn snapshot(&self) -> Vec<u8>;
}

#[derive(Default)]
pub struct SimplePersister {
    states: Mutex<(
        Vec<u8>, // raft state
        Vec<u8>, // snapshot
    )>,
}

impl SimplePersister {
    pub fn new() -> SimplePersister {
        SimplePersister {
            states: Mutex::default(),
        }
    }
}

impl Persister for SimplePersister {
    fn raft_state(&self) -> Vec<u8> {
        self.states.lock().unwrap().0.clone()
    }

    fn save_raft_state(&self, state: Vec<u8>) {
        self.states.lock().unwrap().0 = state;
    }

    fn save_state_and_snapshot(&self, state: Vec<u8>, snapshot: Vec<u8>) {
        self.states.lock().unwrap().0 = state;
        self.states.lock().unwrap().1 = snapshot;
    }

    fn snapshot(&self) -> Vec<u8> {
        self.states.lock().unwrap().1.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_object_safety() {
        let sp = SimplePersister::new();
        sp.save_raft_state(vec![111]);
        let obj: Arc<dyn Persister> = Arc::new(sp);
        assert_eq!(obj.raft_state(), vec![111]);
        obj.save_state_and_snapshot(vec![222], vec![123]);
        assert_eq!(obj.raft_state(), vec![222]);
        assert_eq!(obj.snapshot(), vec![123]);

        let obj1 = obj.clone();
        obj.save_raft_state(vec![233]);
        assert_eq!(obj1.raft_state(), vec![233]);
        assert_eq!(obj1.snapshot(), vec![123]);
    }
}
