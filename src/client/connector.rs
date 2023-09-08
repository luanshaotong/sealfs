use std::{sync::Arc, time::Duration};

use log::info;
use tokio::time::sleep;

use crate::{common::{group_manager::{GroupManager, Group, init_network_connections, init_network_connections_with_servers}, serialization::ClusterStatus}, rpc::client::{RpcClient, TcpStreamCreator}};

pub struct ClientClusterManager {
    pub group_manger: Arc<GroupManager>,
}

impl ClientClusterManager {
    pub fn new(groups: Vec<Group>, client: Arc<
        RpcClient<
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
            TcpStreamCreator,
        >,
    >) -> Self {
        Self {
            group_manger: Arc::new(GroupManager::new(groups, client)),
        }
    }

    pub fn get_target_group(&self, path: &str) -> String {
        let cluster_status = self.group_manger.groups.get_cluster_status();

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return self.group_manger.groups.get_group_id_by_file_path(path);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::Initializing => panic!("cluster status is not ready"),
            ClusterStatus::Idle => todo!(),
            ClusterStatus::NodesStarting => self.group_manger.groups.get_group_id_by_file_path(path),
            ClusterStatus::SyncNewHashRing => self.group_manger.groups.get_group_id_by_file_path(path),
            ClusterStatus::PreTransfer => self.group_manger.groups.get_group_id_by_file_path(path),
            ClusterStatus::Transferring => self.group_manger.groups.get_group_id_by_file_path(path),
            ClusterStatus::PreFinish => self.group_manger.groups.get_new_group_id_by_file_path(path),
            ClusterStatus::Finishing => self.group_manger.groups.get_group_id_by_file_path(path),
            ClusterStatus::StatusError => todo!(),
            ClusterStatus::Unkown => todo!(),
        }
    }
}


pub async fn client_watch_status(
    cluster_manager: Arc<ClientClusterManager>,
) {
    loop {
        match cluster_manager
            .group_manger.groups.get_cluster_status().try_into().unwrap()
        {
            ClusterStatus::SyncNewHashRing => {
                // here I write a long code block to deal with the process from SyncNewHashRing to new Idle status.
                // this is because we don't make persistent flags for status, so we could not check a status is finished or not.
                // so we have to check the status in a long code block, and we could not use a loop to check the status.
                // in the future, we will make persistent flags for status, and we separate the code block for each status.
                info!("Transfer: start to sync new hash ring");
                let all_servers_address = match cluster_manager.group_manger.get_new_hash_ring_info().await {
                    Ok(value) => value,
                    Err(e) => {
                        panic!("Get Hash Ring Info Failed. Error = {}", e);
                    }
                };
                info!("Transfer: get new hash ring info");

                for value in all_servers_address.iter() {
                    if cluster_manager
                    .group_manger.groups
                        .if_group_in_new_hash_ring(&value.0)
                    {
                        continue;
                    }
                    if let Err(e) = cluster_manager.group_manger.add_connection(&value.0).await {
                        // TODO: we should rollback the transfer process
                        panic!("Add Connection Failed. Error = {}", e);
                    }
                }
                cluster_manager
                .group_manger.groups.set_new_hash_ring_info(all_servers_address);
                info!("Transfer: sync new hash ring finished");

                // wait for all servers to be PreTransfer

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    cluster_manager.group_manger.groups.get_cluster_status(),
                )
                .unwrap()
                    == ClusterStatus::SyncNewHashRing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        cluster_manager.group_manger.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::PreTransfer
                );

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    cluster_manager.group_manger.groups.get_cluster_status()
                )
                .unwrap()
                    == ClusterStatus::PreTransfer
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        cluster_manager.group_manger.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::Transferring
                );

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    cluster_manager.group_manger.groups.get_cluster_status()
                )
                .unwrap()
                    == ClusterStatus::Transferring
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        cluster_manager.group_manger.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::PreFinish
                );

                let _old_hash_ring = match cluster_manager
                .group_manger.groups
                    .replace_hash_ring_with_new() {
                    Ok(value) => value,
                    Err(e) => {
                        // TODO: we should rollback the transfer process and deal with the error
                        panic!("Replace Hash Ring Failed. Error = {}", e);
                    }
                };

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    cluster_manager.group_manger.groups.get_cluster_status()
                )
                .unwrap()
                    == ClusterStatus::PreFinish
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        cluster_manager.group_manger.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::Finishing
                );

                let _ = cluster_manager.group_manger.groups.drop_new_hash_ring();
                // here we should close connections to old servers, but now we just wait for remote servers to close connections and do nothing

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    cluster_manager.group_manger.groups.get_cluster_status()
                )
                .unwrap()
                    == ClusterStatus::Finishing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        cluster_manager.group_manger.groups.get_cluster_status()
                    )
                    .unwrap()
                        == ClusterStatus::Idle
                );

                info!("transferring data finished");
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



pub async fn client_init_network_connections(
    manager_address: String,
    connector: Arc<ClientClusterManager>,
) {
    init_network_connections(manager_address, connector.group_manger.clone());
    tokio::spawn(client_watch_status(connector));
}

pub async fn client_init_network_connections_with_servers(
    manager_address: String,
    connector: Arc<ClientClusterManager>,
) {
    init_network_connections_with_servers(manager_address, connector.group_manger.clone());
    tokio::spawn(client_watch_status(connector));
}