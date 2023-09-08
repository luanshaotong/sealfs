// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use anyhow::Error;
use dashmap::DashMap;
use log::info;
use crate::common::group_manager::{GroupsInfo, Group};
use crate::common::serialization::{ClusterStatus, GroupStatus};
pub struct Manager {
    pub group_manager: Arc<Mutex<GroupsInfo>>,
    pub closed: AtomicBool,
    _clients: DashMap<String, String>,
}

impl Manager {
    pub fn new(groups: Vec<Group>) -> Self {
        Self {
            group_manager: Arc::new(Mutex::new(GroupsInfo::new(groups))),
            closed: AtomicBool::new(false),
            _clients: DashMap::new(),
        }
    }

    pub fn set_group_status(&self, group_id: String, status: GroupStatus) -> Option<Error> {

        info!("set group status: {} {:?}", group_id, status);

        match status {
            GroupStatus::Initializing => {
                panic!("cannot set group status to init");
            }
            GroupStatus::PreTransfer => {
                let cluster_status = self.group_manager.lock().unwrap().get_cluster_status();
                if cluster_status != <ClusterStatus as Into<i32>>::into(ClusterStatus::SyncNewHashRing) {
                    return Some(anyhow::anyhow!("cannot pretransfer for group: {}, cluster is not SyncNewHashRing: status: {:?}" , group_id, cluster_status));
                }
                if self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap() != GroupStatus::Finished {
                    return Some(anyhow::anyhow!(
                        "cannot pretransfer for group: {}, group is not finish: status: {:?}",
                        group_id,
                        self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap()
                    ));
                }
                self.group_manager.lock().unwrap().set_group_status(&group_id, status);
                None
            }
            GroupStatus::Transferring => {
                let cluster_status = self.group_manager.lock().unwrap().get_cluster_status();
                if cluster_status != <ClusterStatus as Into<i32>>::into(ClusterStatus::PreTransfer) {
                    return Some(anyhow::anyhow!("cannot transfer for group: {}, cluster is not PreTransfer: status: {:?}" , group_id, cluster_status));
                }
                if self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap() != GroupStatus::PreTransfer {
                    return Some(anyhow::anyhow!(
                        "cannot transfer for group: {}, group is not pretransfer: status: {:?}",
                        group_id,
                        self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap()
                    ));
                }
                self.group_manager.lock().unwrap().set_group_status(&group_id, status);
                None
            }
            GroupStatus::PreFinish => {
                let cluster_status = self.group_manager.lock().unwrap().get_cluster_status();
                if cluster_status != <ClusterStatus as Into<i32>>::into(ClusterStatus::Transferring) {
                    return Some(anyhow::anyhow!("cannot prefinish for group: {}, cluster is not Transferring: status: {:?}" , group_id, cluster_status));
                }
                if self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap() != GroupStatus::Transferring {
                    return Some(anyhow::anyhow!(
                        "cannot prefinish for group: {}, group is not transferring: status: {:?}",
                        group_id,
                        self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap()
                    ));
                }
                self.group_manager.lock().unwrap().set_group_status(&group_id, status);
                None
            }
            GroupStatus::Finishing => {
                let cluster_status = self.group_manager.lock().unwrap().get_cluster_status();
                if cluster_status != <ClusterStatus as Into<i32>>::into(ClusterStatus::PreFinish) {
                    return Some(anyhow::anyhow!("cannot finish for group: {}, cluster is not PreFinish: status: {:?}" , group_id, cluster_status));
                }
                if self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap() != GroupStatus::PreFinish {
                    return Some(anyhow::anyhow!(
                        "cannot finish for group: {}, group is not prefinish: status: {:?}",
                        group_id,
                        self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap()
                    ));
                }
                self.group_manager.lock().unwrap().set_group_status(&group_id, status);
                None
            }
            GroupStatus::Finished => {
                let cluster_status = self.group_manager.lock().unwrap().get_cluster_status();
                match cluster_status.try_into().unwrap() {
                    ClusterStatus::Finishing => {
                        if self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap() != GroupStatus::Finishing {
                            return Some(anyhow::anyhow!(
                                "cannot finish for group: {}, group is not finishing: status: {:?}",
                                group_id,
                                self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap()
                            ));
                        }
                        self.group_manager.lock().unwrap().set_group_status(&group_id, status);
                        None
                    }
                    ClusterStatus::Initializing => {
                        if self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap() != GroupStatus::Initializing {
                            return Some(anyhow::anyhow!(
                                "cannot finish for group: {}, group is not initializing: status: {:?}",
                                group_id,
                                self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap()
                            ));
                        }
                        self.group_manager.lock().unwrap().set_group_status(&group_id, status);
                        None
                    }
                    ClusterStatus::NodesStarting => {
                        if !self
                            .group_manager
                            .lock()
                            .unwrap()
                            .if_group_in_new_hash_ring(&group_id)
                        {
                            return Some(anyhow::anyhow!(
                                "cannot finish for group: {}, group is not in new_hashring",
                                group_id
                            ));
                        }
                        if self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap() != GroupStatus::Initializing {
                            return Some(anyhow::anyhow!(
                                "cannot finish for group: {}, group is not initializing: status: {:?}",
                                group_id,
                                self.group_manager.lock().unwrap().get_group_status(&group_id).unwrap()
                            ));
                        }
                        self.group_manager.lock().unwrap().set_group_status(&group_id, status);
                        None
                    }
                    _ => {
                        Some(anyhow::anyhow!(
                            "cannot finish for group: {}, cluster is not Finishing, Init or AddNodes: status: {:?}",
                            group_id,
                            cluster_status
                        ))
                    }
                }
            }
        }
    }
}
