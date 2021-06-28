use std::collections::HashMap;
use std::time::Duration;

use crate::proto::shardctrlerpb::*;
use crate::shardctrler::{client::Clerk, config, N_SHARDS};

fn check(groups: &[u64], ck: &Clerk) {
    let c = ck.query(None);
    if c.groups.len() != groups.len() {
        panic!("wanted {} groups, got {}", groups.len(), c.groups.len());
    }
    // are the groups as expected?
    for &gid in groups {
        if !c.groups.iter().any(|g| g.gid == gid) {
            panic!("missing group {}", gid);
        }
    }
    // any un-allocated shards?
    if groups.is_empty() {
        for (shard, &gid) in c.shards.iter().enumerate() {
            if !c.groups.iter().any(|g| g.gid == gid) {
                panic!("shard {} -> invalid group {}", shard, gid);
            }
        }
    }
    // more or less balanced sharding?
    let mut counts = HashMap::<u64, usize>::new();
    for &gid in c.shards.iter() {
        *counts.entry(gid).or_default() += 1;
    }
    let min = c.groups.iter().map(|g| counts[&g.gid]).min().unwrap();
    let max = c.groups.iter().map(|g| counts[&g.gid]).max().unwrap();
    assert!(
        max <= min + 1,
        "max {} too much larger than min {}",
        max,
        min
    );
}

fn check_same_config(c1: &Config, c2: &Config) {
    assert_eq!(c1.num, c2.num, "num wrong");
    assert_eq!(c1.shards, c2.shards, "shards wrong");
    assert_eq!(
        c1.groups.len(),
        c2.groups.len(),
        "number of Groups is wrong"
    );
    assert_eq!(c1.groups, c2.groups, "Groups wrong");
}

// helper macro to construct groups
macro_rules! groups {
    ($( $gid:expr => [$($str:expr),*] ),*) => {
        vec![$(Group { gid: $gid, servers: vec![$($str.to_string()),*] }),*]
    }
}

#[test]
fn test_basic_4a() {
    let nservers = 3;
    let cfg = config::Config::new(nservers, false);
    let ck = cfg.make_client(&cfg.all());

    info!("Test: Basic leave/join ...\n");

    let mut cfa = vec![];
    cfa.push(ck.query(None));

    check(&[], &ck);

    let gid1 = 1;
    ck.join(groups!(gid1 => ["x", "y", "z"]));
    check(&[gid1], &ck);
    cfa.push(ck.query(None));

    let gid2 = 2;
    ck.join(groups!(gid2 => ["a", "b", "c"]));
    check(&[gid1, gid2], &ck);
    cfa.push(ck.query(None));

    let cfx = ck.query(None);
    let sa1 = cfx.get_servers(gid1);
    assert_eq!(sa1, ["x", "y", "z"], "wrong servers for gid {}", gid1);
    let sa2 = cfx.get_servers(gid2);
    assert_eq!(sa2, ["a", "b", "c"], "wrong servers for gid {}", gid2);

    ck.leave(vec![gid1]);
    check(&[gid2], &ck);
    cfa.push(ck.query(None));

    ck.leave(vec![gid2]);
    cfa.push(ck.query(None));

    info!("  ... Passed\n");

    info!("Test: Historical queries ...\n");

    for s in 0..nservers {
        cfg.shutdown_server(s);
        for cf in cfa.iter() {
            let c = ck.query(Some(cf.num));
            check_same_config(&c, cf);
        }
        cfg.start_server(s);
        cfg.connect_all();
    }

    info!("  ... Passed\n");

    info!("Test: Move ...\n");

    let gid3 = 503;
    ck.join(groups!(gid3 => ["3a", "3b", "3c"]));
    let gid4 = 504;
    ck.join(groups!(gid4 => ["4a", "4b", "4c"]));
    for i in 0..N_SHARDS {
        let cf = ck.query(None);
        let shard = if i < N_SHARDS / 2 { gid3 } else { gid4 };
        ck.move_(i as u64, shard);
        if cf.shards[i] != shard {
            let cf1 = ck.query(None);
            assert!(cf1.num > cf.num, "Move should increase Config.Num");
        }
    }
    let cf2 = ck.query(None);
    for i in 0..N_SHARDS {
        let shard = if i < N_SHARDS / 2 { gid3 } else { gid4 };
        assert_eq!(
            cf2.shards[i], shard,
            "expected shard {} on gid {} actually {}",
            i, shard, cf2.shards[i]
        );
    }
    ck.leave(vec![gid3]);
    ck.leave(vec![gid4]);

    info!("  ... Passed\n");

    info!("Test: Concurrent leave/join ...\n");

    let npara = 10;
    let gids: Vec<u64> = (0..npara).map(|i| i as u64 * 10 + 100).collect();
    let mut handles = vec![];
    for &gid in gids.iter() {
        let cka = cfg.make_client(&cfg.all());
        handles.push(std::thread::spawn(move || {
            let sid1 = format!("s{}a", gid);
            let sid2 = format!("s{}b", gid);
            cka.join(groups!(gid + 1000 => [sid1]));
            cka.join(groups!(gid => [sid2]));
            cka.leave(vec![gid + 1000]);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    check(&gids, &ck);

    info!("  ... Passed\n");

    info!("Test: Minimal transfers after joins ...\n");

    let c1 = ck.query(None);
    for i in 0..5 {
        let gid = npara + 1 + i;
        ck.join(groups!(gid => [format!("{}a", gid), format!("{}b", gid), format!("{}b", gid)]));
    }
    let c2 = ck.query(None);
    for i in 1..=npara {
        assert_eq!(c1.shards.len(), c2.shards.len());
        for (&s1, &s2) in c1.shards.iter().zip(c2.shards.iter()) {
            if s2 == i && s1 != i {
                panic!("non-minimal transfer after Join()s");
            }
        }
    }

    info!("  ... Passed\n");

    info!("Test: Minimal transfers after leaves ...\n");

    for i in 0..5 {
        ck.leave(vec![npara + 1 + i]);
    }
    let c3 = ck.query(None);
    for i in 1..=npara {
        for j in 0..c1.shards.len() {
            if c2.shards[j] == i && c3.shards[j] != i {
                panic!("non-minimal transfer after Leave()s");
            }
        }
    }

    info!("  ... Passed\n");
}

#[test]
fn test_multi_4a() {
    let nservers = 3;
    let cfg = config::Config::new(nservers, false);
    let ck = cfg.make_client(&cfg.all());

    info!("Test: Multi-group leave/join ...\n");

    let mut cfa = vec![];
    cfa.push(ck.query(None));

    check(&[], &ck);

    let gid1 = 1;
    let gid2 = 2;
    ck.join(groups!(gid1 => ["x", "y", "z"], gid2 => ["a", "b", "c"]));
    check(&[gid1, gid2], &ck);
    cfa.push(ck.query(None));

    let gid3 = 3;
    ck.join(groups!(gid3 => ["j", "k", "l"]));
    check(&[gid1, gid2, gid3], &ck);
    cfa.push(ck.query(None));

    let cfx = ck.query(None);
    let sa1 = cfx.get_servers(gid1);
    assert_eq!(sa1, ["x", "y", "z"], "wrong servers for gid {}", gid1);
    let sa2 = cfx.get_servers(gid2);
    assert_eq!(sa2, ["a", "b", "c"], "wrong servers for gid {}", gid2);
    let sa3 = cfx.get_servers(gid3);
    assert_eq!(sa3, ["j", "k", "l"], "wrong servers for gid {}", gid3);

    ck.leave(vec![gid1, gid3]);
    check(&[gid2], &ck);
    cfa.push(ck.query(None));

    let cfx = ck.query(None);
    let sa2 = cfx.get_servers(gid2);
    assert_eq!(sa2, ["a", "b", "c"], "wrong servers for gid {}", gid2);

    ck.leave(vec![gid2]);

    info!("  ... Passed\n");

    info!("Test: Concurrent multi leave/join ...\n");

    let npara = 10;
    let gids: Vec<u64> = (0..npara).map(|i| i as u64 + 1000).collect();
    let mut handles = vec![];
    for &gid in gids.iter() {
        let cka = cfg.make_client(&cfg.all());
        handles.push(std::thread::spawn(move || {
            cka.join(groups!(
                gid => [
                    format!("{}a", gid),
                    format!("{}b", gid),
                    format!("{}c", gid)
                ],
                gid + 1000 => [format!("{}a", gid + 1000)],
                gid + 2000 => [format!("{}a", gid + 2000)]
            ));
            cka.leave(vec![gid + 1000, gid + 2000]);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    check(&gids, &ck);

    info!("  ... Passed\n");

    info!("Test: Minimal transfers after multijoins ...\n");

    let c1 = ck.query(None);
    let mut m = vec![];
    for i in 0..5 {
        let gid = npara + 1 + i;
        m.append(&mut groups!(gid => [format!("{}a", gid), format!("{}b", gid)]));
    }
    ck.join(m);
    let c2 = ck.query(None);
    for i in 1..=npara {
        assert_eq!(c1.shards.len(), c2.shards.len());
        for (&s1, &s2) in c1.shards.iter().zip(c2.shards.iter()) {
            if s2 == i && s1 != i {
                panic!("non-minimal transfer after Join()s");
            }
        }
    }

    info!("  ... Passed\n");

    info!("Test: Minimal transfers after multileaves ...\n");

    let l: Vec<u64> = (0..5).map(|i| npara + 1 + i).collect();
    ck.leave(l);
    let c3 = ck.query(None);
    for i in 1..=npara {
        for j in 0..c1.shards.len() {
            if c2.shards[j] == i && c3.shards[j] != i {
                panic!("non-minimal transfer after Leave()s");
            }
        }
    }

    info!("  ... Passed\n");

    info!("Test: Check Same config on servers ...\n");

    let leader = cfg.leader().expect("Leader not found");
    let c = ck.query(None); // Config leader claims
    cfg.shutdown_server(leader);

    let mut attempts = 0;
    while cfg.leader().is_ok() {
        attempts += 1;
        assert!(attempts < 3, "Leader not found");
        std::thread::sleep(Duration::from_secs(1));
    }

    let c1 = ck.query(None);
    check_same_config(&c, &c1);

    info!("  ... Passed\n");
}
