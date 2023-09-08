use std::{sync::{Arc, atomic::Ordering}, time::Duration};

use log::{error, info};
use parking_lot::RwLock;
use tokio::time::sleep;

use crate::{common::{group_manager::{GroupManager, Group, hash_ring::HashRing}, serialization::{ClusterStatus, GroupStatus, ManagerOperationType}, sender::REQUEST_TIMEOUT, errors::CONNECTION_ERROR}, rpc::client::{RpcClient, TcpStreamCreator}};

pub struct ServerClusterManager {
    pub connection_manager: Arc<GroupManager>,
    pub group_id: String,
    pub self_role_in_group: RwLock<String>, // 0: primary, 1: secondary
}

impl ServerClusterManager {
    pub fn new(groups: Vec<Group>, client: Arc<
        RpcClient<
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
            TcpStreamCreator,
        >,
    >) -> Self {
        Self {
            connection_manager: Arc::new(GroupManager::new(groups, client)),
            group_id: "".to_string(),
            self_role_in_group: RwLock::new("".to_string()),
        }
    }

    pub async fn update_group_status(&self, group_status: GroupStatus) -> Result<(), i32> {
        if self.self_role_in_group.read().as_str() == "secondary" {
            return Ok(());
        }
        let send_meta_data = bincode::serialize(&group_status).unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .connection_manager
            .sender
            .client
            .call_remote(
                &self.connection_manager.groups.get_manager_address().unwrap(),
                ManagerOperationType::UpdateGroupStatus.into(),
                0,
                &self.group_id,
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
                error!("update group status failed, error: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    
    pub fn get_target_group(&self, path: &str) -> (String, bool) {
        let cluster_status = self.connection_manager.groups.get_cluster_status();

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return (self.connection_manager.groups.get_group_id_by_file_path(path), false);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::Initializing => todo!(),
            ClusterStatus::Idle => todo!(),
            ClusterStatus::NodesStarting => (self.connection_manager.groups.get_group_id_by_file_path(path), false),
            ClusterStatus::SyncNewHashRing => (self.connection_manager.groups.get_group_id_by_file_path(path), false),
            ClusterStatus::PreTransfer => {
                let address = self.connection_manager.groups.get_group_id_by_file_path(path);
                if address != self.group_id {
                    (address, false)
                } else {
                    let new_address = self.connection_manager.groups.get_new_group_id_by_file_path(path);
                    if new_address != self.group_id {
                        // the most efficient way is to check the operation_type
                        // if operation_type is Create, forward the request to the new node
                        // here is a temporary solution
                        match self.meta_engine.is_exist(path) {
                            Ok(true) => (address, false),
                            Ok(false) => (new_address, false),
                            Err(e) => {
                                error!("get forward address failed, error: {}", e);
                                (new_address, false) // local db error, attempt to forward. but it may cause inconsistency
                            }
                        }
                    } else {
                        (address, false)
                    }
                }
            }
            ClusterStatus::Transferring => {
                let address = self.connection_manager.groups.get_group_id_by_file_path(path);
                if address != self.group_id {
                    (address, false)
                } else {
                    let new_address = self.connection_manager.groups.get_new_group_id_by_file_path(path);
                    if new_address != self.group_id {
                        match self.transfer_manager.status(path) {
                            Some(true) => (new_address, false),
                            Some(false) => (address, false),
                            None => (new_address, false),
                        }
                    } else {
                        (address, false)
                    }
                }
            }
            ClusterStatus::PreFinish => {
                let address = self.connection_manager.groups.get_group_id_by_file_path(path);
                if address != self.group_id {
                    (address, false)
                } else {
                    let new_address = self.connection_manager.groups.get_new_group_id_by_file_path(path);
                    if new_address != self.group_id {
                        (new_address, false)
                    } else {
                        (address, false)
                    }
                }
            }
            ClusterStatus::Finishing => (self.connection_manager.groups.get_group_id_by_file_path(path), false),
            ClusterStatus::StatusError => todo!(),
            ClusterStatus::Unkown => todo!(),
            //s => panic!("get forward address failed, invalid cluster status: {}", s),
        }
    }

    pub fn get_forward_group(&self, path: &str) -> (Option<String>, bool) {
        let cluster_status = self.connection_manager.groups.get_cluster_status();

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            // assert!(self.address == self.connection_manager.groups.get_group_id_by_file_path(path));
            return (None, false);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::NodesStarting => (None, false),
            ClusterStatus::SyncNewHashRing => (None, false),
            ClusterStatus::PreTransfer => {
                let address = self.connection_manager.groups.get_new_group_id_by_file_path(path);
                if address != self.group_id {
                    // the most efficient way is to check the operation_type
                    // if operation_type is Create, forward the request to the new node
                    // here is a temporary solution
                    match self.meta_engine.is_exist(path) {
                        Ok(true) => (None, false),
                        Ok(false) => (Some(address), false),
                        Err(e) => {
                            error!("get forward address failed, error: {}", e);
                            (Some(address), false) // local db error, attempt to forward. but it may cause inconsistency
                        }
                    }
                } else {
                    (None, false)
                }
            }
            ClusterStatus::Transferring => {
                let address = self.connection_manager.groups.get_new_group_id_by_file_path(path);
                if address != self.group_id {
                    match self.transfer_manager.status(path) {
                        Some(true) => (Some(address), false),
                        Some(false) => (None, false),
                        None => (Some(address), false),
                    }
                } else {
                    (None, false)
                }
            }
            ClusterStatus::PreFinish => {
                let address = self.connection_manager.groups.get_new_group_id_by_file_path(path);
                if address != self.group_id {
                    (Some(address), false)
                } else {
                    (None, false)
                }
            }
            ClusterStatus::Finishing => (None, false),
            ClusterStatus::Initializing => (None, false),
            s => panic!("get forward address failed, invalid cluster status: {}", s),
        }
    }
}


pub async fn watch_status(connector: Arc<ServerClusterManager>) {
    loop {
        match connector.connection_manager.groups.get_cluster_status().try_into().unwrap()
        {
            ClusterStatus::SyncNewHashRing => {
                info!("watch status: start to sync new hash ring");
                let groups_info = match connector.connection_manager.get_new_hash_ring_info().await {
                    Ok(value) => value,
                    Err(e) => {
                        panic!("Get Hash Ring Info Failed. Error = {}", e);
                    }
                };
                info!("watch status: get new hash ring info");
                for value in groups_info.iter() {
                    if connector.group_id == value.0
                        || connector.connection_manager.groups.if_group_in_hash_ring(&value.0)
                    {
                        continue;
                    }
                    if let Err(e) = connector.connection_manager.add_connection(&value.0).await {
                        // TODO: rollback the transfer process
                        panic!("watch status: add connection failed, error = {}", e);
                    }
                }
                connector.connection_manager.groups.set_new_hash_ring(HashRing::new(groups_info));
                info!("watch status: sync new hash ring finished");
                match connector.update_group_status(GroupStatus::PreTransfer).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    connector.connection_manager.groups.get_cluster_status(),
                )
                .unwrap()
                    == ClusterStatus::SyncNewHashRing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        connector.connection_manager.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::PreTransfer
                );

                let file_map = connector.make_up_file_map();

                info!("watch status: start to transfer files");
                match connector
                    .update_group_status(GroupStatus::Transferring)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }
                while <i32 as TryInto<ClusterStatus>>::try_into(
                    connector.connection_manager.groups.get_cluster_status(),
                )
                .unwrap()
                    == ClusterStatus::PreTransfer
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        connector.connection_manager.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::Transferring
                );

                if let Err(e) = connector.transfer_files(file_map).await {
                    panic!("transfer files failed, error = {}", e);
                }

                info!("watch status: transfer files finished");
                match connector.update_group_status(GroupStatus::PreFinish).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    connector.connection_manager.groups.get_cluster_status(),
                )
                .unwrap()
                    == ClusterStatus::Transferring
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        connector.connection_manager.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::PreFinish
                );

                let _old_hash_ring = match connector
                    .connection_manager
                    .groups
                    .replace_hash_ring_with_new()
                {
                    Ok(value) => value,
                    Err(e) => {
                        // TODO: _old_hash_ring should be used to rollback the transfer process and deal with the error
                        panic!("Replace Hash Ring Failed. Error = {}", e);
                    }
                };

                info!("watch status: start to finishing");
                match connector.update_group_status(GroupStatus::Finishing).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    connector.connection_manager.groups.get_cluster_status(),
                )
                .unwrap()
                    == ClusterStatus::PreFinish
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        connector.connection_manager.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::Finishing
                );

                let _ = connector.connection_manager.groups.take_new_hash_ring();
                // here we should close connections to old groups, but now we just wait for remote groups to close connections and do nothing

                info!("watch status: start to finishing");
                match connector.update_group_status(GroupStatus::Finished).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    connector.connection_manager.groups.get_cluster_status(),
                )
                .unwrap()
                    == ClusterStatus::Finishing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        connector.connection_manager.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::Idle
                );

                info!("watch status: transferring data finished");
            }
            ClusterStatus::Idle => {
                sleep(Duration::from_secs(1)).await;
            }
            ClusterStatus::Initializing => {
                sleep(Duration::from_secs(1)).await;
            }
            ClusterStatus::NodesStarting => {
                sleep(Duration::from_secs(1)).await;
            }
            e => {
                panic!("cluster status error: {:?}", e as u32);
            }
        }
    }
}