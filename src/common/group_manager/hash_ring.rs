// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use conhash::{ConsistentHash, Node};

#[derive(Clone)]
pub struct GroupNode {
    pub group_id: String,
}

impl Node for GroupNode {
    fn name(&self) -> String {
        self.group_id.clone()
    }
}

pub struct HashRing {
    pub ring: ConsistentHash<GroupNode>,
    pub groups: HashMap<String, usize>,
}

impl Clone for HashRing {
    fn clone(&self) -> Self {
        let groups = self.groups.clone();
        let mut ring = ConsistentHash::<GroupNode>::new();
        for (group, weight) in groups.iter() {
            ring.add(
                &GroupNode {
                    group_id: group.clone(),
                },
                *weight,
            );
        }
        HashRing { ring, groups }
    }
}

impl HashRing {
    pub fn new(groups: Vec<(String, usize)>) -> Self {
        let mut ring = ConsistentHash::<GroupNode>::new();
        let mut groups_map = HashMap::new();
        for (group_id, weight) in groups {
            ring.add(
                &GroupNode {
                    group_id: group_id.clone(),
                },
                weight,
            );
            groups_map.insert(group_id, weight);
        }
        HashRing {
            ring,
            groups: groups_map,
        }
    }

    pub fn get(&self, key: &str) -> Option<&GroupNode> {
        self.ring.get_str(key)
    }

    pub fn add(&mut self, group: GroupNode, weight: usize) {
        self.ring.add(&group, weight);
        self.groups.insert(group.group_id, weight);
    }

    pub fn remove(&mut self, group: &GroupNode) {
        self.ring.remove(group);
        self.groups.remove(&group.group_id);
    }

    pub fn contains(&self, group: &str) -> bool {
        self.groups.contains_key(group)
    }

    pub fn get_group_lists(&self) -> Vec<String> {
        self.groups.keys().cloned().collect()
    }
}
