use dashmap::DashMap;

use super::serialization::{ServerStatus, GroupType};

pub struct Server {
    pub status: ServerStatus,
}

pub struct Group {
    pub name: String,
    pub servers: Vec<(String, Server)>,
    pub primary: String,
    pub secondaries: Vec<String>,
    pub group_status: GroupType,
}

pub struct GroupManager {
    pub groups: DashMap<String, Group>,
}

impl GroupManager {
    pub fn new(servers: Vec<(String, Vec<(String, ServerStatus)>)>) -> Self {
        let groups = DashMap::new();
        for (group_id, servers) in servers {
            let mut group = Group {
                name: group_id.clone(),
                servers: vec![],
                primary: "".to_string(),
                secondaries: vec![],
                group_status: GroupType::Running,
            };
            for server in servers {
                group.servers.push((server.0, Server { status: server.1 }));
            }
            group.primary = group.servers[0].0.clone();
            for i in 1..group.servers.len() {
                group.secondaries.push(group.servers[i].0.clone());
            }
            groups.insert(group_id, group);
        }
        GroupManager {
            groups,
        }
    }

    pub fn add_group(&self, group: Group) {
        self.groups.insert(group.name.clone(), group);
    }

    pub fn get_primary(&self, group_id: &str) -> Option<String> {
        let group = self.groups.get(group_id)?;
        Some(group.primary.clone())
    }

    pub fn get_groups_info(&self) -> Vec<(String, Vec<(String, ServerStatus)>)> {
        let mut groups_info = vec![];
        for group in self.groups.iter() {
            let mut servers = vec![];
            for server in group.value().servers.iter() {
                servers.push((server.0.clone(), server.1.status.clone()));
            }
            groups_info.push((group.key().clone(), servers));
        }
        groups_info
    }
}