// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

// sender is used to send requests to the other sealfs servers

use std::{sync::Arc, time::Duration};

use log::{error, debug};

use crate::{
    common::{errors::CONNECTION_ERROR, serialization::{AddGroupsSendMetaData, CreateVolumeSendMetaData, DeleteGroupsSendMetaData,
        GetClusterStatusRecvMetaData, GetHashRingInfoRecvMetaData, ManagerOperationType, OperationType,
        Volume,
    }},
    rpc::client::{RpcClient, TcpStreamCreator},
};

use super::{ errors::{SERVER_IS_NOT_LEADER, self}, Group, GroupsInfo};

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const CONTROLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Sender {
    pub client: Arc<
        RpcClient<
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
            TcpStreamCreator,
        >,
    >,
    pub group_manager: Arc<GroupsInfo>,
}

impl Sender {
    pub fn new(
        client: Arc<
            RpcClient<
                tokio::net::tcp::OwnedReadHalf,
                tokio::net::tcp::OwnedWriteHalf,
                TcpStreamCreator,
            >,
        >,
        group_manager: Arc<GroupsInfo>,
    ) -> Self {
        Sender { client , group_manager}
    }

    pub async fn server_is_primary(&self, address: &str) -> Result<bool, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
             .client
             .call_remote(
                address,
                OperationType::ServerIsPrimary.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                CONTROLL_REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status == SERVER_IS_NOT_LEADER {
                    Ok(false)
                } else if status != 0 {
                    Err(status)
                } else {
                    Ok(true)
                }
            }
            Err(e) => {
                error!("server is primary failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn swap_primary_server(
        &self,
        group_id: &str,
        last_primary_server_address: &str,
    ) -> Result<(), i32> {
        if last_primary_server_address != self
            .group_manager
            .get_primary_server_in_group(group_id)
            .unwrap()
        {
            return Err(errors::SERVER_IS_NOT_LEADER);
        };
        for server in self.group_manager.get_group_servers(group_id).unwrap() {
            match self.server_is_primary(&server.server_address).await {
                Ok(true) => {
                    if let Some(e) = self.group_manager
                        .set_primary_server_in_group(group_id, server.server_address) {
                        error!("set primary server in group failed: {}", e);
                        return Err(errors::GROUP_MANAGER_ERROR);
                    }
                    return Ok(());
                }
                Ok(false) => continue,
                Err(_) => todo!(),
            }
        }
        Err(errors::SERVER_IS_NOT_LEADER)
    }

    pub async fn call_remote_by_group(
        &self,
        group_id: &str,
        operation_type: u32,
        req_flags: u32,
        path: &str,
        send_meta_data: &[u8],
        send_data: &[u8],
        status: &mut i32,
        rsp_flags: &mut u32,
        recv_meta_data_length: &mut usize,
        recv_data_length: &mut usize,
        recv_meta_data: &mut [u8],
        recv_data: &mut [u8],
        timeout: Duration,
    ) -> Result<(), i32> {
        loop {
            let address = self.group_manager.get_primary_server_in_group(group_id).unwrap();
            let result = self
                .client
                .call_remote(
                    &address,
                    operation_type.into(),
                    req_flags,
                    path,
                    send_meta_data,
                    send_data,
                    status,
                    rsp_flags,
                    recv_meta_data_length,
                    recv_data_length,
                    recv_meta_data,
                    recv_data,
                    timeout,
                )
                .await;
            match result {
                Ok(_) => {
                    if status == &SERVER_IS_NOT_LEADER {
                        debug!("{} is not leader, try to get new primary", &address);
                        loop {
                            if let Err(e) = self.swap_primary_server(group_id, &address).await {
                                if e != SERVER_IS_NOT_LEADER {
                                    return Err(e);
                                }
                                // new primary server is not ready, wait for a while
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            } else {
                                // get new primary server
                                break;
                            }
                        } 
                    } else {
                        return Ok(());
                    }
                }
                Err(e) => {
                    error!("call remote failed: {}", e);
                    return Err(CONNECTION_ERROR);
                }
            }
        }
    }

    pub async fn add_new_groups(
        &self,
        manager_address: &str,
        new_groups_info: Vec<Group>,
    ) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let send_meta_data =
            bincode::serialize(&AddGroupsSendMetaData { new_groups_info }).unwrap();

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::AddNodes.into(),
                0,
                "",
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("add new groups failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn delete_groups(
        &self,
        manager_address: &str,
        deleted_groups_info: Vec<String>,
    ) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let send_meta_data = bincode::serialize(&DeleteGroupsSendMetaData {
            deleted_groups_info,
        })
        .unwrap();

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::RemoveNodes.into(),
                0,
                "",
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("delete groups failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn get_cluster_status(&self, manager_address: &str) -> Result<i32, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 4];

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::GetClusterStatus.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    let cluster_status_meta_data: GetClusterStatusRecvMetaData =
                        bincode::deserialize(&recv_meta_data).unwrap();
                    Ok(cluster_status_meta_data.status)
                }
            }
            Err(e) => {
                error!("get cluster status failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn get_hash_ring_info(
        &self,
        manager_address: &str,
    ) -> Result<Vec<(String, usize)>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::GetHashRing.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => {
                error!("get hash ring info failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn get_new_hash_ring_info(
        &self,
        manager_address: &str,
    ) -> Result<Vec<(String, usize)>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::GetNewHashRing.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => {
                error!("get new hash ring info failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn get_groups_info(
        &self,
        manager_address: &str,
    ) -> Result<Vec<Group>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data = vec![0u8; 65535];

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::GetGroups.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                CONTROLL_REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                let groups_info: Vec<Group> =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(groups_info)
            }
            Err(e) => {
                error!("get replica sets info failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn list_volumes(&self, group_id: &str) -> Result<Vec<Volume>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
             .call_remote_by_group(
                group_id,
                OperationType::ListVolumes.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                CONTROLL_REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                let volumes: Vec<Volume> =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(volumes)
            }
            Err(e) => {
                error!("list volumes failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn create_volume(&self, group_id: &str, name: &str, size: u64) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let send_meta_data = bincode::serialize(&CreateVolumeSendMetaData { size }).unwrap();

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
             .call_remote_by_group(
                group_id,
                OperationType::CreateVolume.into(),
                0,
                name,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("create volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn delete_volume(&self, group_id: &str, name: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
             .call_remote_by_group(
                group_id,
                OperationType::DeleteVolume.into(),
                0,
                name,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("delete volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn clean_volume(&self, group_id: &str, name: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
             .call_remote_by_group(
                group_id,
                OperationType::CleanVolume.into(),
                0,
                name,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                CONTROLL_REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("clean volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn init_volume(&self, group_id: &str, name: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
             .call_remote_by_group(
                group_id,
                OperationType::InitVolume.into(),
                0,
                name,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("init volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn create_no_parent(
        &self,
        group_id: &str,
        operation_type: OperationType,
        parent: &str,
        send_meta_data: &[u8],
    ) -> Result<Vec<u8>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self
             .call_remote_by_group(
                group_id,
                operation_type.into(),
                0,
                parent,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(recv_meta_data[..recv_meta_data_length].to_vec())
                }
            }
            Err(e) => {
                error!("create file failed with error: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn delete_no_parent(
        &self,
        group_id: &str,
        operation_type: OperationType,
        parent: &str,
        send_meta_data: &[u8],
    ) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self
             .call_remote_by_group(
                group_id,
                operation_type.into(),
                0,
                parent,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("create file failed with error: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn directory_add_entry(
        &self,
        group_id: &str,
        path: &str,
        send_meta_data: &[u8],
    ) -> Result<(), i32> {
        let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
            (0, 0, 0, 0);
        let result = self
             .call_remote_by_group(
                group_id,
                OperationType::DirectoryAddEntry as u32,
                0,
                path,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            e => {
                error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn directory_delete_entry(
        &self,
        group_id: &str,
        path: &str,
        send_meta_data: &[u8],
    ) -> Result<(), i32> {
        let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
            (0, 0, 0, 0);
        let result = self
             .call_remote_by_group(
                group_id,
                OperationType::DirectoryDeleteEntry as u32,
                0,
                path,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            e => {
                error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                Err(CONNECTION_ERROR)
            }
        }
    }
}
