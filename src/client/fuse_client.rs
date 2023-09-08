// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::group_manager::Group;
use crate::common::group_manager::sender::{Sender, REQUEST_TIMEOUT};
use crate::common::serialization::{
    file_attr_as_bytes_mut, CreateDirSendMetaData, CreateFileSendMetaData,
    DeleteDirSendMetaData, DeleteFileSendMetaData, OpenFileSendMetaData, OperationType,
    ReadDirSendMetaData, ReadFileSendMetaData, Volume, WriteFileSendMetaData,
};
use crate::common::util::{empty_dir, empty_file};
use crate::rpc::client::RpcClient;
use dashmap::DashMap;
use fuser::{
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite,
};
use libc::{mode_t, DT_DIR, DT_LNK, DT_REG};
use log::debug;
use std::ffi::{OsStr, OsString};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use super::connector::ClientClusterManager;
const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct FSClient {
    pub sender: Arc<Sender>,
    pub inodes: DashMap<String, u64>,
    pub inodes_reverse: DashMap<u64, String>,
    pub inode_counter: std::sync::atomic::AtomicU64,
    pub fd_counter: std::sync::atomic::AtomicU64,
    pub handle: tokio::runtime::Handle,
    pub cluster_manager: Arc<ClientClusterManager>,
}

impl Default for FSClient {
    fn default() -> Self {
        Self::new()
    }
}

impl FSClient {
    pub fn new() -> Self {
        let info_syncer = Arc::new(ClientClusterManager::new(vec![], Arc::new(RpcClient::default())));
        Self {
            sender: info_syncer.group_manger.sender.clone(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            inode_counter: std::sync::atomic::AtomicU64::new(1),
            fd_counter: std::sync::atomic::AtomicU64::new(1),
            handle: tokio::runtime::Handle::current(),
            cluster_manager: info_syncer,
        }
    }

    pub fn get_new_inode(&self) -> u64 {
        debug!(
            "get new inode, inode_counter: {}",
            self.inode_counter
                .load(std::sync::atomic::Ordering::Acquire)
        );
        self.inode_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    pub fn get_new_fd(&self) -> u64 {
        self.fd_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    pub async fn init_volume(&self, volume_name: &str) -> Result<u64, i32> {
        let inode = self.get_new_inode();
        self.inodes_reverse.insert(inode, volume_name.to_string());
        self.inodes.insert(volume_name.to_string(), inode);
        self.sender
            .init_volume(&self.cluster_manager.get_target_group(volume_name), volume_name)
            .await?;
        Ok(inode)
    }

    pub async fn list_volumes(&self) -> Result<Vec<Volume>, i32> {
        let mut volumes: Vec<Volume> = Vec::new();

        for group in self.cluster_manager.group_manger.groups.get_groups_info() {
            let mut new_volumes = self.sender.list_volumes(&group.group_id).await?;
            volumes.append(&mut new_volumes);
        }
        Ok(volumes)
    }

    pub async fn delete_servers(&self, servers_info: Vec<String>) -> Result<(), i32> {
        self.sender
            .delete_groups(&self.cluster_manager.group_manger.groups.get_manager_address().unwrap(), servers_info)
            .await
    }

    pub fn get_full_path(&self, parent: &str, name: &OsStr) -> String {
        let path = format!("{}/{}", parent, name.to_str().unwrap());
        path
    }

    pub async fn create_volume(&self, name: &str, size: u64) -> Result<(), i32> {
        self.sender
            .create_volume(&self.cluster_manager.get_target_group(name), name, size)
            .await
    }

    pub async fn delete_volume(&self, name: &str) -> Result<(), i32> {
        self.sender
            .delete_volume(&self.cluster_manager.get_target_group(name), name)
            .await
    }

    pub async fn add_new_groups(&self, new_groups_info: Vec<Group>) -> Result<(), i32> {
        self.sender
            .add_new_groups(&self.cluster_manager.group_manger.groups.get_manager_address().unwrap(), new_groups_info)
            .await
    }

    pub async fn lookup_remote(&self, parent: u64, name: OsString, reply: ReplyEntry) {
        debug!(
            "lookup_remote, parent: {}, name: {}",
            parent,
            name.to_str().unwrap()
        );
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => self.get_full_path(parent_path.deref(), &name),
            None => {
                reply.error(libc::ENOENT);
                debug!("lookup_remote error: {:?}", libc::ENOENT);
                return;
            }
        };
        let group_id = self.cluster_manager.get_target_group(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut file_attr = Box::new(empty_file());
        let recv_meta_data = file_attr_as_bytes_mut(&mut file_attr);

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::GetFileAttr.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                debug!("lookup_remote status: {}", status);
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "lookup_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );

                if self.inodes.contains_key(&path) {
                    file_attr.ino = *self.inodes.get(&path).unwrap().value();
                } else {
                    file_attr.ino = self.get_new_inode();
                    self.inodes.insert(path.clone(), file_attr.ino);
                    self.inodes_reverse.insert(file_attr.ino, path.clone());
                }

                reply.entry(&TTL, &file_attr, 0);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn create_remote(
        &self,
        parent: u64,
        name: OsString,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!("create_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("create_remote error");
                return;
            }
        };
        let group_id = self.cluster_manager.get_target_group(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut file_attr = Box::new(empty_file());
        let recv_meta_data = file_attr_as_bytes_mut(&mut file_attr);

        let send_meta_data = bincode::serialize(&CreateFileSendMetaData {
            mode,
            umask,
            flags,
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::CreateFile.into(),
                0,
                &path,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "create_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );
                // let mut file_attr: FileAttr = {
                //     let file_attr_simple: FileAttrSimple =
                //     FileAttrSimple::from_bytes(&recv_meta_data[..recv_meta_data_length]).unwrap();
                //     file_attr_simple.into()
                // };

                file_attr.ino = self.get_new_inode();

                let path = self.get_full_path(&path, &name);
                self.inodes.insert(path.clone(), file_attr.ino);
                self.inodes_reverse.insert(file_attr.ino, path);

                reply.created(&TTL, &file_attr, 0, 0, 0);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn getattr_remote(&self, ino: u64, reply: ReplyAttr) {
        debug!("getattr_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("getattr_remote error");
                return;
            }
        };
        debug!("getattr_remote path: {:?}", path);
        let group_id = self.cluster_manager.get_target_group(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut file_attr = Box::new(empty_file());
        let recv_meta_data = file_attr_as_bytes_mut(&mut file_attr);

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::GetFileAttr.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "getattr_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );
                // let mut file_attr: FileAttr = {
                //     let file_attr_simple: FileAttrSimple =
                //     FileAttrSimple::from_bytes(&recv_meta_data[..recv_meta_data_length]).unwrap();
                //     file_attr_simple.into()
                // };
                debug!("getattr_remote file_attr: {:?}", file_attr);
                if self.inodes.contains_key(&path) {
                    file_attr.ino = *self.inodes.get(&path).unwrap().value();
                } else {
                    file_attr.ino = self.get_new_inode();
                    self.inodes.insert(path.clone(), file_attr.ino);
                    self.inodes_reverse.insert(file_attr.ino, path.clone());
                }
                reply.attr(&TTL, &file_attr);
                debug!("getattr_remote success");
            }
            Err(_) => {
                debug!("getattr_remote error");
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn readdir_remote(&self, ino: u64, offset: i64, mut reply: ReplyDirectory) {
        debug!("readdir_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("readdir_remote error");
                return;
            }
        };
        let size = 2048;

        let group_id = self.cluster_manager.get_target_group(&path);
        let md = ReadDirSendMetaData {
            offset,
            size: size as u32,
        };
        let send_meta_data = bincode::serialize(&md).unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_data = vec![0u8; size];

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::ReadDir.into(),
                0,
                &path,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut recv_data,
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "readdir_remote recv_data: {:?}",
                    &recv_data[..recv_data_length]
                );
                let mut total = 0;
                let mut offset = offset;
                while total < recv_data_length {
                    let r#type = u8::from_le_bytes(recv_data[total..total + 1].try_into().unwrap());
                    let name_len =
                        u16::from_le_bytes(recv_data[total + 1..total + 3].try_into().unwrap());
                    let name = String::from_utf8(
                        recv_data[total + 3..total + 3 + name_len as usize]
                            .try_into()
                            .unwrap(),
                    )
                    .unwrap();
                    let kind = match r#type {
                        DT_REG => fuser::FileType::RegularFile,
                        DT_DIR => fuser::FileType::Directory,
                        DT_LNK => fuser::FileType::Symlink,
                        _ => fuser::FileType::RegularFile,
                    };
                    offset += 1;
                    let r = reply.add(1, offset, kind, name);
                    if r {
                        break;
                    }

                    total += 3 + name_len as usize;
                }

                reply.ok();
                debug!("readdir_remote success");
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn read_remote(&self, ino: u64, offset: i64, size: u32, reply: ReplyData) {
        debug!("read_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("read_remote error");
                return;
            }
        };
        let group_id = self.cluster_manager.get_target_group(&path);

        let meta_data = bincode::serialize(&ReadFileSendMetaData { offset, size }).unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_data = vec![0u8; size as usize];

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::ReadFile.into(),
                0,
                &path,
                &meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut recv_data,
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(()) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "read_remote success recv_data: {:?}",
                    &recv_data[..recv_data_length]
                );
                reply.data(&recv_data);
            }
            Err(e) => {
                debug!("read_remote error: {:?}", e);
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn write_remote(&self, ino: u64, offset: i64, data: Vec<u8>, reply: ReplyWrite) {
        debug!("write_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("write_remote error");
                return;
            }
        };
        debug!("write_remote path: {:?}, data_len: {}", path, data.len());
        let group_id = self.cluster_manager.get_target_group(&path);
        let send_meta_data = bincode::serialize(&WriteFileSendMetaData { offset }).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 4];

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::WriteFile.into(),
                0,
                &path,
                &send_meta_data,
                &data,
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
            Ok(()) => {
                let size: u32 =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                debug!("write_remote success, size: {}", size);
                reply.written(size);
            }
            Err(_) => {
                debug!("write_remote error");
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn mkdir_remote(&self, parent: u64, name: OsString, _mode: u32, reply: ReplyEntry) {
        debug!("mkdir_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("mkdir_remote error");
                return;
            }
        };
        debug!("mkdir_remote ,path: {:?}", &path);
        let group_id = self.cluster_manager.get_target_group(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut file_attr = Box::new(empty_dir());
        let recv_meta_data = file_attr_as_bytes_mut(&mut file_attr);

        let mode: mode_t = 0o755;
        let send_meta_data = bincode::serialize(&CreateDirSendMetaData {
            mode,
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::CreateDir.into(),
                0,
                &path,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "create_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );
                // let mut file_attr: FileAttr = {
                //     let file_attr_simple: FileAttrSimple =
                //     FileAttrSimple::from_bytes(&recv_meta_data[..recv_meta_data_length]).unwrap();
                //     file_attr_simple.into()
                // };

                file_attr.ino = self.get_new_inode();

                reply.entry(&TTL, &file_attr, 0);

                let path = self.get_full_path(&path, &name);
                self.inodes.insert(path.clone(), file_attr.ino);
                self.inodes_reverse.insert(file_attr.ino, path);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn open_remote(&self, ino: u64, flags: i32, reply: ReplyOpen) {
        debug!("open_remote");
        if flags & libc::O_CREAT != 0 {
            todo!("open_remote O_CREAT") // this is not supported by the fuse crate
        }
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("open_remote error");
                return;
            }
        };
        let group_id = self.cluster_manager.get_target_group(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mode: mode_t = if flags & libc::O_WRONLY != 0 {
            libc::S_IWUSR
        } else if flags & libc::O_RDWR != 0 {
            libc::S_IWUSR | libc::S_IRUSR
        } else {
            libc::S_IRUSR
        };

        let send_meta_data = bincode::serialize(&OpenFileSendMetaData { flags, mode }).unwrap();

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::OpenFile.into(),
                0,
                &path,
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
            Ok(()) => {
                reply.opened(self.get_new_fd(), 0);
            }
            Err(e) => {
                debug!("open_remote error: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn unlink_remote(&self, parent: u64, name: OsString, reply: ReplyEmpty) {
        debug!("unlink_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("unlink_remote error");
                return;
            }
        };
        let group_id = self.cluster_manager.get_target_group(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let send_meta_data = bincode::serialize(&DeleteFileSendMetaData {
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::DeleteFile.into(),
                0,
                &path,
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
                let path = self.get_full_path(&path, &name);
                self.inodes_reverse
                    .remove(self.inodes.get(&path).as_deref().unwrap());
                self.inodes.remove(&path);
                reply.ok();
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub async fn rmdir_remote(&self, parent: u64, name: OsString, reply: ReplyEmpty) {
        debug!("rmdir_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                debug!("rmdir_remote error");
                return;
            }
        };
        let group_id = self.cluster_manager.get_target_group(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let send_meta_data = bincode::serialize(&DeleteDirSendMetaData {
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

        let result = self
             .sender
             .call_remote_by_group(
                &group_id,
                OperationType::DeleteDir.into(),
                0,
                &path,
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
                let path = self.get_full_path(&path, &name);
                self.inodes_reverse
                    .remove(self.inodes.get(&path).as_deref().unwrap());
                self.inodes.remove(&path);
                reply.ok();
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }
}
