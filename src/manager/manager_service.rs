// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use crate::{
    common::{serialization::{
        AddGroupsSendMetaData, ClusterStatus, DeleteGroupsSendMetaData, GetClusterStatusRecvMetaData,
        GetHashRingInfoRecvMetaData, ManagerOperationType, GroupStatus, GetGroupsRecvMetaData,
    }, group_manager::Group},
    rpc::server::Handler,
};

use super::core::Manager;

use async_trait::async_trait;
use log::{debug, error, info};

pub struct ManagerService {
    pub manager: Arc<Manager>,
}

pub async fn update_group_status(manager: Arc<Manager>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if manager.closed.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let group_manager = manager.group_manager.lock().unwrap();
        let status = group_manager.get_cluster_status();
        debug!("current cluster status is {:?}", status);
        match status.try_into().unwrap() {
            ClusterStatus::Idle => {}
            ClusterStatus::NodesStarting => {
                // if all groups is ready, change the cluster status to SyncNewHashRing
                if group_manager.all_servers_ready_for(GroupStatus::Finished) {
                    group_manager.set_cluster_status(ClusterStatus::SyncNewHashRing.into());
                    info!("all groups is ready, change the cluster status to SyncNewHashRing");
                };
            }
            ClusterStatus::SyncNewHashRing => {
                // if all groups is ready, change the cluster status to PreTransfer
                if group_manager.all_servers_ready_for(GroupStatus::PreTransfer) {
                    group_manager.set_cluster_status(ClusterStatus::PreTransfer.into());
                    info!("all groups is ready, change the cluster status to PreTransfer");
                }
            }
            ClusterStatus::PreTransfer => {
                // if all groups is ready, change the cluster status to Transferring
                let flag = group_manager.all_servers_ready_for(GroupStatus::Transferring);
                if flag {
                    group_manager.set_cluster_status(ClusterStatus::Transferring.into());
                    info!("all groups is ready, change the cluster status to Transferring");
                }
            }
            ClusterStatus::Transferring => {
                // if all groups is ready, change the cluster status to PreFinish
                let flag = group_manager.all_servers_ready_for(GroupStatus::PreFinish);
                if flag {
                    group_manager.set_cluster_status(ClusterStatus::PreFinish.into());
                    info!("all groups is ready, change the cluster status to PreFinish");
                }
            }
            ClusterStatus::PreFinish => {
                // if all groups is ready, change the cluster status to Finishing
                if group_manager.all_servers_ready_for(GroupStatus::Finished) {
                    match group_manager.replace_hash_ring_with_new() {
                        Ok(_) => {}
                        Err(e) => {
                            // TODO: rollback the transfer process and deal with the error
                            panic!("replace hash ring with new failed, error: {}", e);
                        }
                    }
                    group_manager.set_cluster_status(ClusterStatus::Finishing.into());
                    info!("all groups is ready, change the cluster status to Finishing");
                }
            }
            ClusterStatus::Finishing => {
                // if all groups is ready, change the cluster status to Idle
                if group_manager.all_servers_ready_for(GroupStatus::Finished) {
                    // move new_hashring to hashring
                    group_manager.remove_groups_not_in_new_hash_ring();
                    let _ = group_manager.take_new_hash_ring();
                    group_manager.set_cluster_status(ClusterStatus::Idle.into());
                    info!("all groups is ready, change the cluster status to Idle");
                }
            }
            ClusterStatus::Initializing => {
                // if all groups is ready, change the cluster status to Idle
                if group_manager.all_servers_ready_for(GroupStatus::Finished) {
                    group_manager.set_cluster_status(ClusterStatus::Idle.into());
                    info!("all groups is ready, change the cluster status to Idle");
                }
            }
            s => panic!("update group status failed, invalid cluster status: {}", s),
        }
    }
}

impl ManagerService {
    pub fn new(groups: Vec<Group>) -> Self {
        let manager = Arc::new(Manager::new(groups));
        ManagerService { manager }
    }
}

#[async_trait]
impl Handler for ManagerService {
    async fn dispatch(
        &self,
        id: u32,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        _data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        let r#type = ManagerOperationType::try_from(operation_type).unwrap();
        match r#type {
            ManagerOperationType::GetClusterStatus => {
                let status = self.manager.group_manager.lock().unwrap().get_cluster_status();
                let response_meta_data =
                    bincode::serialize(&GetClusterStatusRecvMetaData { status }).unwrap();

                debug!("connection {} get cluster status: {:?}", id, status);

                Ok((
                    0,
                    0,
                    response_meta_data.len(),
                    0,
                    response_meta_data,
                    Vec::new(),
                ))
            }
            ManagerOperationType::GetHashRing => {
                let hash_ring_info = self.manager.group_manager.lock().unwrap().get_hash_ring_info();

                info!("connection {} get hash ring: {:?}", id, hash_ring_info);

                let response_meta_data =
                    bincode::serialize(&GetHashRingInfoRecvMetaData { hash_ring_info }).unwrap();
                Ok((
                    0,
                    0,
                    response_meta_data.len(),
                    0,
                    response_meta_data,
                    Vec::new(),
                ))
            }
            ManagerOperationType::GetNewHashRing => match self.manager.group_manager.lock().unwrap().get_new_hash_ring_info() {
                Some(hash_ring_info) => {
                    info!("connection {} get new hash ring: {:?}", id, hash_ring_info);
                    let response_meta_data =
                        bincode::serialize(&GetHashRingInfoRecvMetaData { hash_ring_info })
                            .unwrap();
                    Ok((
                        0,
                        0,
                        response_meta_data.len(),
                        0,
                        response_meta_data,
                        Vec::new(),
                    ))
                }
                None => {
                    error!("connection {} get new hash ring failed, new hash ring is empty", id);
                    Ok((libc::ENOENT, 0, 0, 0, Vec::new(), Vec::new()))
                }
            },
            ManagerOperationType::AddNodes => {
                let new_groups_info = bincode::deserialize::<AddGroupsSendMetaData>(&metadata)
                    .unwrap()
                    .new_groups_info;
                info!("connection {} add nodes: {:?}", id, new_groups_info.iter().map(|g| g.group_id.clone()).collect::<Vec<String>>());
                match self.manager.group_manager.lock().unwrap().add_groups(new_groups_info) {
                    None => Ok((0, 0, 0, 0, Vec::new(), Vec::new())),
                    Some(e) => {
                        error!("add nodes error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, Vec::new(), Vec::new()))
                    }
                }
            }
            ManagerOperationType::RemoveNodes => {
                let deleted_groups_info =
                    bincode::deserialize::<DeleteGroupsSendMetaData>(&metadata)
                        .unwrap()
                        .deleted_groups_info;
                info!("connection {} remove nodes: {:?}", id, deleted_groups_info);
                match self.manager.group_manager.lock().unwrap().delete_nodes(deleted_groups_info) {
                    None => Ok((0, 0, 0, 0, Vec::new(), Vec::new())),
                    Some(e) => {
                        error!("remove nodes error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, Vec::new(), Vec::new()))
                    }
                }
            }
            ManagerOperationType::UpdateGroupStatus => {
                info!("connection {} update group status", id);
                match self.manager.group_manager.lock().unwrap().set_group_status(
                    std::str::from_utf8(&path).unwrap(),
                    bincode::deserialize(&metadata).unwrap(),
                ) {
                    None => Ok((0, 0, 0, 0, Vec::new(), Vec::new())),
                    Some(e) => {
                        error!("update group status error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, Vec::new(), Vec::new()))
                    }
                }
            }
            ManagerOperationType::GetGroups => {
                info!("connection {} get replica sets", id);
                let groups_info = self.manager.group_manager.lock().unwrap().get_groups_info();
                let response_meta_data =
                    bincode::serialize(&GetGroupsRecvMetaData { groups_info }).unwrap();
                Ok((
                    0,
                    0,
                    response_meta_data.len(),
                    0,
                    response_meta_data,
                    Vec::new(),
                ))
            }
            _ => todo!(),
        }
    }
}
