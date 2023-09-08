pub mod hash_ring;
pub mod sender;

use dashmap::DashMap;
use serde::{Serialize, Deserialize};

use self::{hash_ring::HashRing, sender::Sender};

use super::{serialization::GroupStatus, errors::CHANGE_CLUSTER_WHILE_NOT_IDLE};

use std::{
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{debug, error, info};
use spin::RwLock;
use tokio::time::sleep;

use crate::{common::{errors::{self, status_to_string, CONNECTION_ERROR}, group_manager::hash_ring::GroupNode, serialization::ClusterStatus}, rpc::client::{RpcClient, TcpStreamCreator}};

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct ServerInfo {
    pub server_id: String,
    pub server_address: String,
}

impl ServerInfo {
    pub fn new(server_id: String, server_address: String) -> Self {
        Self {
            server_id,
            server_address,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct Group {
    pub group_id: String,
    pub primary_address: String,
    pub servers: Vec<ServerInfo>,
    pub group_status: GroupStatus,
    pub weight: usize,
}

impl Group {
    pub fn new(
        group_id: String,
        primary_address: String,
        servers: Vec<ServerInfo>,
        group_status: GroupStatus,
        weight: usize,
    ) -> Self {
        Self {
            group_id,
            primary_address,
            servers,
            group_status,
            weight,
        }
    }
}

pub struct GroupsInfo {
    cluster_status: AtomicI32,
    groups: DashMap<String, Group>, // group_id -> group
    hash_ring: Arc<RwLock<Option<HashRing>>>,
    new_hash_ring: Arc<RwLock<Option<HashRing>>>,
    manager_address: Arc<RwLock<Option<String>>>,
}

impl GroupsInfo {
    pub fn new(groups_info: Vec<Group>) -> Self {
        let mut groups = DashMap::new();
        let mut group_locks = DashMap::new();
        for group in groups_info {
            groups.insert(group.group_id, group);
            group_locks.insert(group.group_id, Arc::new(RwLock::new(())));
        }
        Self {
            cluster_status: AtomicI32::new(0),
            groups,
            hash_ring: Arc::new(RwLock::new(None)),
            new_hash_ring: Arc::new(RwLock::new(None)),
            manager_address: Arc::new(RwLock::new(None)),
        }
    }

    pub fn sync_groups(&self, groups_info: Vec<Group>) {
        for group in groups_info {
            self.add_group(group);
        }
        for kv in self.groups.iter() {
            if !groups_info.iter().any(|value| value.group_id == kv.key().clone()) {
                self.delete_group(kv.key());
            }
        }
    }

    pub fn add_group(&self, group: Group) {
        self.groups.insert(group.group_id.clone(), group);
    }

    pub fn delete_group(&self, group_id: &str) {
        self.groups.remove(group_id);
    }

    pub fn get_primary_server_in_group(&self, group_id: &str) -> Option<String> {
        let group = self.groups.get(group_id)?;
        Some(group.primary_address.clone())
    }

    pub fn set_primary_server_in_group(
        &self,
        group_id: &str,
        primary_server_address: String,
    ) -> Option<String> {
        let mut group = match self.groups.get_mut(group_id) {
            Some(group) => group,
            None => return Some("group not found".to_string()),
        };
        group.primary_address = primary_server_address;
        None
    }

    pub fn get_groups_info(&self) -> Vec<Group> {
        self.groups.iter().map(|kv| kv.value().clone()).collect()
    }

    pub fn get_group_status(&self, group_id: &str) -> Option<GroupStatus> {
        let group = self.groups.get(group_id)?;
        Some(group.group_status)
    }

    pub fn get_group_servers(&self, group_id: &str) -> Option<Vec<ServerInfo>> {
        let group = self.groups.get(group_id)?;
        Some(group.servers.clone())
    }

    pub fn set_group_status(&self, group_id: &str, group_status: GroupStatus) -> Option<String> {
        let mut group = match self.groups.get_mut(group_id) {
            Some(group) => group,
            None => return Some("group not found".to_string()),
        };
        group.group_status = group_status;
        None
    }

    pub fn get_cluster_status(&self) -> i32 {
        self.cluster_status.load(Ordering::Relaxed)
    }

    pub fn set_cluster_status(&self, cluster_status: i32) {
        self.cluster_status.store(cluster_status, Ordering::Relaxed);
    }

    pub fn get_manager_address(&self) -> Option<String> {
        self.manager_address.read().clone()
    }

    pub fn set_manager_address(&self, manager_address: String) {
        self.manager_address.write().replace(manager_address);
    }

    pub fn get_group_id_by_file_path(&self, file_path: &str) -> String {
        self.hash_ring
            .read()
            .as_ref()
            .unwrap()
            .get(file_path)
            .unwrap()
            .group_id
            .clone()
    }

    pub fn get_new_group_id_by_file_path(&self, file_path: &str) -> String {
        match self.new_hash_ring.read().as_ref() {
            Some(hash_ring) => {
                hash_ring.get(file_path).unwrap().group_id.clone()
            }
            None => self.get_group_id_by_file_path(file_path)
        }
    }

    pub fn get_hash_ring_info(&self) -> Vec<(String, usize)> {
        self.hash_ring
            .read()
            .unwrap()
            .groups
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    pub fn get_new_hash_ring_info(&self) -> Option<Vec<(String, usize)>> {
        if let Some(new_hashring) = self.new_hash_ring.read().as_ref()
         {
            Some(new_hashring
                .groups
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect())
        } else {
            None
        }
    }

    pub fn set_hash_ring_info(&self, hash_ring_info: Vec<(String, usize)>) {
        let mut hash_ring = self.hash_ring.write();
        hash_ring.replace(HashRing::new(hash_ring_info));
    }

    pub fn set_new_hash_ring_info(&self, hash_ring_info: Vec<(String, usize)>) {
        let mut new_hash_ring = self.new_hash_ring.write();
        new_hash_ring.replace(HashRing::new(hash_ring_info));
    }

    pub fn set_hash_ring(&self, hash_ring: HashRing) -> Option<HashRing> {
        self.hash_ring.write().replace(hash_ring)
    }

    pub fn set_new_hash_ring(&self, hash_ring: HashRing) -> Option<HashRing> {
        self.new_hash_ring.write().replace(hash_ring)
    }

    pub fn replace_hash_ring_with_new(&self) -> Result<Option<HashRing>, i32> {
        let mut hash_ring = self.hash_ring.write();
        match self.new_hash_ring.read().as_ref() {
            Some(new_hash_ring) => {
                hash_ring.replace(new_hash_ring.clone());
                Ok(None)
            }
            None => Err(errors::NEW_HASH_RING_IS_EMPTY),
        }
    }

    pub fn drop_new_hash_ring(&self) -> Option<HashRing> {
        let mut new_hash_ring = self.new_hash_ring.write();
        new_hash_ring.take()
    }

    pub fn add_groups(&self, groups: Vec<Group>) -> Option<i32> {
        info!("add_nodes: {:?}", groups.iter().map(|g| g.group_id.clone()).collect::<Vec<String>>());
        if self.get_cluster_status() != <ClusterStatus as Into<i32>>::into(ClusterStatus::Idle) {
            return Some(CHANGE_CLUSTER_WHILE_NOT_IDLE);
        }
        let mut new_hash_ring = self.hash_ring.read().unwrap().clone();
        for group in groups {
            new_hash_ring.add(
                GroupNode {
                    group_id: group.group_id.clone(),
                },
                group.weight,
            );
            self.add_group(group);
        }

        self.set_new_hash_ring(new_hash_ring);
        self.set_cluster_status(ClusterStatus::NodesStarting.into());

        None
    }

    pub fn delete_nodes(&self, nodes: Vec<String>) -> Option<i32> {
        if self.get_cluster_status() != <ClusterStatus as Into<i32>>::into(ClusterStatus::Idle) {
            return Some(CHANGE_CLUSTER_WHILE_NOT_IDLE);
        }
        let mut new_hash_ring = self.hash_ring.read().unwrap().clone();
        new_hash_ring.remove(&GroupNode {
            group_id: nodes[0].clone(),
        });

        self.set_new_hash_ring(new_hash_ring);

        self.set_cluster_status(ClusterStatus::NodesStarting.into());
        None
    }

    pub fn if_group_in_hash_ring(&self, group_id: &str) -> bool {
        self.hash_ring
            .read()
            .as_ref()
            .unwrap()
            .contains(&group_id.to_string())
    }

    pub fn if_group_in_new_hash_ring(&self, group_id: &str) -> bool {

        match self.new_hash_ring
            .read()
            .as_ref()
        {
            Some(new_hash_ring) => new_hash_ring.contains(&group_id.to_string()),
            None => false,
        }
    }

    pub fn all_servers_ready_for(&self, group_status: GroupStatus) -> bool {
        for group in self.groups.iter() {
            if group.value().group_status != group_status {
                return false;
            }
        }
        true
    }

    pub fn remove_groups_not_in_new_hash_ring(&self) {
        for group in self.groups.iter() {
            if !self.if_group_in_new_hash_ring(group.key()) {
                self.delete_group(group.key());
            }
        }
    }

    pub fn take_new_hash_ring(&self) -> Option<HashRing> {
        self.new_hash_ring.write().take()
    }
}


pub struct GroupManager {
    pub groups: Arc<GroupsInfo>,
    pub sender: Arc<Sender>,
}

impl GroupManager {

    pub fn new(groups: Vec<Group>, client: Arc<
        RpcClient<
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
            TcpStreamCreator,
        >,
    >) -> Self {
        let group_manager = Arc::new(GroupsInfo::new(groups));
        Self {
            groups: group_manager.clone(),
            sender: Arc::new(Sender::new(client.clone(), group_manager)),
        }
    }
 
    pub async fn get_cluster_status(&self) -> Result<i32, i32> {
        self.sender
            .get_cluster_status(&self.groups.get_manager_address().unwrap())
            .await
    }

    pub async fn get_hash_ring_info(&self) -> Result<Vec<(String, usize)>, i32> {
        self.sender
            .get_hash_ring_info(&self.groups.get_manager_address().unwrap())
            .await
    }
    pub async fn get_new_hash_ring_info(&self) -> Result<Vec<(String, usize)>, i32> {
        self.sender
            .get_new_hash_ring_info(&self.groups.get_manager_address().unwrap())
            .await
    }

    pub async fn get_groups_info_from_remote(&self) -> Result<Vec<Group>, i32> {
        self.sender
            .get_groups_info(&self.groups.get_manager_address().unwrap())
            .await
    }

    pub async fn add_connection(&self, server_address: &str) -> Result<(), i32> {
        self.sender.client
            .add_connection(server_address)
            .await
            .map_err(|e| {
                error!("add connection failed: {:?}", e);
                CONNECTION_ERROR
            })
    }

    pub fn remove_connection(&self, server_address: &str) {
        self.sender.client.remove_connection(server_address);
    }

    pub async fn connect_to_manager(&self, manager_address: &str) -> Result<(), i32> {
        self.groups.set_manager_address(manager_address.to_string());
        self.add_connection(manager_address).await.map_err(|e| {
            error!("add connection failed: {:?}", e);
            CONNECTION_ERROR
        })
    }

    pub async fn connect_servers(&self) -> Result<(), i32> {
        debug!("init");

        let all_servers_address = async {
            loop {
                match self
                    .groups.get_cluster_status().try_into().unwrap()
                {
                    ClusterStatus::Idle => {
                        return self.get_hash_ring_info().await;
                    }
                    ClusterStatus::Initializing => {
                        info!("cluster is initalling, wait for a while");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    ClusterStatus::PreFinish => {
                        info!("cluster is initalling, wait for a while");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    s => {
                        error!("invalid cluster status: {}", s);
                        return Err(errors::INVALID_CLUSTER_STATUS);
                    }
                }
            }
        }
        .await?;

        let groups_info = self.get_groups_info_from_remote().await?;
        for value in groups_info.iter() {
            for value in value.servers.iter() {
                if let Err(e) = self.add_connection(&value.server_address.clone()).await {
                    panic!("Init: Add Connection Failed. Error = {}", e);  // TODO: should mark and skip the failed server
                }
            }
        }
        
        self.groups.set_hash_ring(HashRing::new(all_servers_address));
        self.groups.sync_groups(groups_info);
        Ok(())
    }

}

async fn sync_cluster_infos(groups: Arc<GroupsInfo>) {
    loop {
        {
            let result = groups.get_cluster_status();
            if groups.cluster_status.load(Ordering::Relaxed) != result {
                groups.cluster_status.store(result, Ordering::Relaxed);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

pub async fn init_network_connections(
    manager_address: String,
    connection_manager: Arc<GroupManager>,
) {
    if let Err(e) = connection_manager.connect_to_manager(&manager_address).await {
        panic!("connect to manager failed, err = {}", status_to_string(e));
    }
    tokio::spawn(sync_cluster_infos(connection_manager.groups.clone()));
}

pub async fn init_network_connections_with_servers(
    manager_address: String,
    connection_manager: Arc<GroupManager>,
) {
    if let Err(e) = connection_manager.connect_to_manager(&manager_address).await {
        panic!("connect to manager failed, err = {}", status_to_string(e));
    }
    
    info!("connect_servers");
    if let Err(status) = connection_manager.connect_servers().await {
        panic!(
            "connect_servers failed, status = {:?}",
            status_to_string(status)
        );
    }

    tokio::spawn(sync_cluster_infos(connection_manager.groups.clone()));
}
