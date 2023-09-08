// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod storage_engine;
mod transfer_manager;
pub mod journal;
pub mod connector;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use async_trait::async_trait;
use log::{debug, error, info};
use storage_engine::StorageEngine;
use tokio::time::sleep;

use crate::{
    common::{
        errors::status_to_string,
        serialization::{
            bytes_as_file_attr, ClusterStatus, CreateDirSendMetaData, CreateFileSendMetaData,
            CreateVolumeSendMetaData, DeleteDirSendMetaData, DeleteFileSendMetaData,
            DirectoryEntrySendMetaData, OpenFileSendMetaData, OperationType, ReadDirSendMetaData,
            GroupStatus, TruncateFileSendMetaData,
        },
        serialization::{ReadFileSendMetaData, WriteFileSendMetaData},
    },
    rpc::server::{Handler, RpcServer},
    server::storage_engine::meta_engine::MetaEngine,
};
use distributed_engine::DistributedEngine;
use storage_engine::file_engine::FileEngine;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ServerError {
    #[error("ParseHeaderError")]
    ParseHeaderError,
}

pub async fn run(
    database_path: String,
    storage_path: String,
    group_id: String,
    server_address: String,
    manager_address: String,
    #[cfg(feature = "disk-db")] cache_capacity: usize,
    #[cfg(feature = "disk-db")] write_buffer_size: usize,
) -> anyhow::Result<()> {
    debug!("run server");
    let meta_engine = Arc::new(MetaEngine::new(
        &database_path,
        #[cfg(feature = "disk-db")]
        cache_capacity,
        #[cfg(feature = "disk-db")]
        write_buffer_size,
    ));
    let storage_engine = Arc::new(FileEngine::new(&storage_path, Arc::clone(&meta_engine)));
    storage_engine.init();
    info!("Init: Storage Engine Init Finished");

    let engine = Arc::new(DistributedEngine::new(
        group_id,
        server_address.clone(),
        storage_engine,
        meta_engine,
    ));

    info!("Init: Connect To Manager: {}", manager_address);
    if let Err(e) = engine.client.add_connection(&manager_address).await {
        panic!("Connect To Manager Failed, Error = {}", e);
    }
    engine.cluster_manager.set_manager_address(manager_address);

    tokio::spawn(sync_cluster_status(Arc::clone(&engine)));

    while <i32 as TryInto<ClusterStatus>>::try_into(engine.cluster_status.load(Ordering::Relaxed))
        .unwrap()
        == ClusterStatus::Unkown
    {
        sleep(Duration::from_secs(1)).await;
    }

    let handler = Arc::new(FileRequestHandler::new(engine.clone()));
    let server = RpcServer::new(handler, &server_address);

    let engine_clone = Arc::clone(&engine);

    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            error!("Server Run Failed, Error = {}", e);
            engine_clone.closed.store(true, Ordering::Relaxed);
        }
    });

    info!("Init: Add connections and update group Status");

    let hash_ring_info = match engine.get_hash_ring_info().await {
        Ok(value) => value,
        Err(_) => {
            panic!("Get Hash Ring Info Failed.");
        }
    };
    info!("Init: Hash Ring Info: {:?}", hash_ring_info);

    let groups_info = match engine.get_groups_info().await {
        Ok(value) => value,
        Err(_) => {
            panic!("Get Groups Info Failed.");
        }
    };

    for value in groups_info.iter() {
        for value in value.2.iter() {
            if &server_address == value {
                continue;
            }
            if let Err(e) = engine.add_connection(value).await {
                panic!("Init: Add Connection Failed. Error = {}", e);
            }
        }
    }
    info!("Init: Add Connections Success.");
    engine
        .hash_ring
        .write()
        .replace(HashRing::new(hash_ring_info));
    info!("Init: Update Hash Ring Success.");
    engine.cluster_manager.sync(groups_info);

    match <i32 as TryInto<ClusterStatus>>::try_into(engine.cluster_status.load(Ordering::Relaxed))
        .unwrap()
    {
        ClusterStatus::Initializing => {
            match engine.update_group_status(GroupStatus::Finished).await {
                Ok(_) => {
                    info!("Update Group Status to Finish Success.");
                }
                Err(e) => {
                    panic!("Update Group Status to Finish Failed. Error = {}", e);
                }
            }
        }
        ClusterStatus::Idle => {}
        e => {
            panic!("Cluster Status Unexpected. Status = {:?}", e as u32);
        }
    }
    info!("Init: Start Transferring Data.");
    watch_status(engine.clone()).await;

    Ok(())
}

pub struct FileRequestHandler<S: StorageEngine + std::marker::Send + std::marker::Sync + 'static> {
    engine: Arc<DistributedEngine<S>>,
}

impl<S: StorageEngine> FileRequestHandler<S>
where
    S: StorageEngine + std::marker::Send + std::marker::Sync + 'static,
{
    pub fn new(engine: Arc<DistributedEngine<S>>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl<S: StorageEngine> Handler for FileRequestHandler<S>
where
    S: StorageEngine + std::marker::Send + std::marker::Sync + 'static,
{
    // dispatch is the main function to handle the request from client
    // the return value is a tuple of (i32, u32, Vec<u8>, Vec<u8>)
    // the first i32 is the status of the function
    // the second u32 is the reserved field flags
    // the third Vec<u8> is the metadata of the function
    // the fourth Vec<u8> is the data of the function
    async fn dispatch(
        &self,
        id: u32,
        operation_type: u32,
        flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        let r#type = match OperationType::try_from(operation_type) {
            Ok(value) => value,
            Err(e) => {
                error!("Operation Type Error: {:?}", e);
                return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
            }
        };

        let file_path = unsafe { std::str::from_utf8_unchecked(&path) };

        // this lock is deprecated, and always return false
        let _lock =
            match self.engine.get_forward_address(file_path) {
                (Some(address), _) => {
                    match self
                        .engine
                        .forward_request(address, operation_type, flags, file_path, data, metadata)
                        .await
                    {
                        Ok(value) => {
                            return Ok(value);
                        }
                        Err(e) => {
                            debug!(
                            "Forward Request Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            return Ok((e, 0, 0, 0, Vec::new(), Vec::new()));
                        }
                    }
                }
                (None, lock) => lock,
            };

        match r#type {
            OperationType::Unkown => {
                error!("Unkown Operation Type: path: {}", file_path);
                Ok((-1, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::Lookup => {
                error!("{} Lookup not implemented", self.engine.group_id);
                Ok((-1, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CreateFile => {
                debug!("{} Create File: path: {}", self.engine.group_id, file_path);
                let meta_data_unwraped: CreateFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self
                    .engine
                    .create_file(
                        metadata,
                        file_path,
                        &meta_data_unwraped.name,
                        meta_data_unwraped.flags,
                        meta_data_unwraped.umask,
                        meta_data_unwraped.mode,
                    )
                    .await
                {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::CreateDir => {
                debug!("{} Create Dir: path: {}", self.engine.group_id, file_path);
                let meta_data_unwraped: CreateDirSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self
                    .engine
                    .create_dir(
                        metadata,
                        file_path,
                        &meta_data_unwraped.name,
                        meta_data_unwraped.mode,
                    )
                    .await
                {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            e, file_path, operation_type, flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::GetFileAttr => {
                debug!("{} Get File Attr: path: {}", self.engine.group_id, file_path);
                let (return_meta_data, status) =
                    match self.engine.get_file_attr(file_path) {
                        Ok(value) => (value, 0),
                        Err(e) => {
                            debug!(
                            "Get File Attr Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            (Vec::new(), e)
                        }
                    };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::OpenFile => {
                debug!("{} Open File {}", self.engine.group_id, file_path);
                let meta_data_unwraped: OpenFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let status = match self.engine.open_file(
                    file_path,
                    meta_data_unwraped.flags,
                    meta_data_unwraped.mode,
                ) {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Open File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::ReadDir => {
                debug!("{} Read Dir: {}", self.engine.group_id, file_path);
                let md: ReadDirSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (data, status) = match self.engine.read_dir(file_path, md.size, md.offset) {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Read Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((status, 0, 0, data.len(), Vec::new(), data))
            }
            OperationType::ReadFile => {
                debug!("{} Read File: {}", self.engine.group_id, file_path);
                let md: ReadFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (data, status) = match self.engine.read_file(file_path, md.size, md.offset) {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Read File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((status, 0, 0, data.len(), Vec::new(), data))
            }
            OperationType::WriteFile => {
                debug!("{} Write File: {}", self.engine.group_id, file_path);
                let md: WriteFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (status, size) =
                    match self
                        .engine
                        .write_file(file_path, data.as_slice(), md.offset)
                    {
                        Ok(size) => (0, size as u32),
                        Err(e) => {
                            debug!(
                                "Write File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                                status_to_string(e),
                                file_path,
                                operation_type,
                                flags
                            );
                            (e, 0)
                        }
                    };
                Ok((
                    status,
                    0,
                    size.to_le_bytes().len(),
                    0,
                    size.to_le_bytes().to_vec(),
                    Vec::new(),
                ))
            }
            OperationType::DeleteFile => {
                debug!("{} Delete File: {}", self.engine.group_id, file_path);
                let meta_data_unwraped: DeleteFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let status = match self
                    .engine
                    .delete_file(metadata, file_path, &meta_data_unwraped.name)
                    .await
                {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::DeleteDir => {
                debug!("{} Delete Dir: {}", self.engine.group_id, file_path);
                let meta_data_unwraped: DeleteDirSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let status = match self
                    .engine
                    .delete_dir(metadata, file_path, &meta_data_unwraped.name)
                    .await
                {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::DirectoryAddEntry => {
                debug!("{} Directory Add Entry: {}", self.engine.group_id, file_path);
                let md: DirectoryEntrySendMetaData = bincode::deserialize(&metadata).unwrap();
                Ok((
                    self.engine
                        .directory_add_entry(file_path, md.file_name, md.file_type),
                    0,
                    0,
                    0,
                    vec![],
                    vec![],
                ))
            }
            OperationType::DirectoryDeleteEntry => {
                debug!(
                    "{} Directory Delete Entry: {}",
                    self.engine.group_id, file_path
                );
                let md: DirectoryEntrySendMetaData = bincode::deserialize(&metadata).unwrap();
                Ok((
                    self.engine
                        .directory_delete_entry(file_path, md.file_name, md.file_type),
                    0,
                    0,
                    0,
                    vec![],
                    vec![],
                ))
            }
            OperationType::TruncateFile => {
                debug!("{} Truncate File: {}", self.engine.group_id, file_path);
                let md: TruncateFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let status =
                    match self.engine.truncate_file(file_path, md.length) {
                        Ok(()) => 0,
                        Err(e) => {
                            debug!(
                            "Truncate File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            e
                        }
                    };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CheckFile => {
                info!("{} Checkout File: {}", self.engine.group_id, file_path);
                let file_attr = bytes_as_file_attr(&metadata);
                let status =
                    match self.engine.check_file(file_path, file_attr) {
                        Ok(()) => 0,
                        Err(e) => {
                            info!(
                            "Checkout File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            e
                        }
                    };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CheckDir => {
                info!("{} Checkout Dir: {}", self.engine.group_id, file_path);
                let file_attr = bytes_as_file_attr(&metadata);
                let status =
                    match self.engine.check_dir(file_path, file_attr) {
                        Ok(()) => 0,
                        Err(e) => {
                            info!(
                            "Checkout Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            e
                        }
                    };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CreateDirNoParent => {
                debug!(
                    "{} Create Dir no Parent: path: {}",
                    self.engine.group_id, file_path
                );
                let meta_data_unwraped: CreateDirSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self
                    .engine
                    .create_dir_no_parent(file_path, meta_data_unwraped.mode)
                {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::CreateFileNoParent => {
                debug!(
                    "{} Create File no Parent: path: {}",
                    self.engine.group_id, file_path
                );
                let meta_data_unwraped: CreateFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self.engine.create_file_no_parent(
                    file_path,
                    meta_data_unwraped.flags,
                    meta_data_unwraped.umask,
                    meta_data_unwraped.mode,
                ) {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::DeleteDirNoParent => {
                debug!(
                    "{} Delete Dir no Parent: {}",
                    self.engine.group_id, file_path
                );
                let status = match self.engine.delete_dir_no_parent(file_path) {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::DeleteFileNoParent => {
                debug!(
                    "{} Delete File no Parent: {}",
                    self.engine.group_id, file_path
                );
                let status = match self.engine.delete_file_no_parent(file_path) {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CreateVolume => {
                info!("{} Create Volume", self.engine.group_id);
                let meta_data_unwraped: CreateVolumeSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                info!("Create Volume: {:?}, id: {}", file_path, id);
                if file_path.is_empty()
                    || file_path.len() > 255
                    || file_path.contains('\0')
                    || file_path.contains('/')
                {
                    return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
                }
                let status = match self
                    .engine
                    .create_volume(file_path, meta_data_unwraped.size)
                {
                    Ok(()) => 0,
                    Err(e) => {
                        info!(
                            "Create Volume Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            std::str::from_utf8(path.as_slice()).unwrap(),
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                return Ok((status, 0, 0, 0, Vec::new(), Vec::new()));
            }
            OperationType::InitVolume => {
                info!(
                    "{} Init Volume: {}, id: {}",
                    self.engine.group_id, file_path, id
                );
                if !file_path.is_empty()
                    && self.engine.get_address(file_path) == self.engine.group_id
                    && self.engine.meta_engine.init_volume(file_path).is_err()
                {
                    error!(
                        "Volume not Exists: id: {}, file_path: {}, address {}, self_address {}",
                        id,
                        file_path,
                        self.engine.get_address(file_path),
                        self.engine.group_id
                    );
                    return Ok((libc::ENOENT, 0, 0, 0, vec![], vec![]));
                }
                //self.engine.volume_indexes.insert(id, file_path);
                return Ok((0, 0, 0, 0, Vec::new(), Vec::new()));
            }
            OperationType::ListVolumes => {
                info!("{} List Volume", self.engine.group_id);
                let return_meta_data = self.engine.meta_engine.list_volumes().unwrap();
                return Ok((
                    0,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ));
            }
            OperationType::DeleteVolume => {
                info!("{} Delete Volume", self.engine.group_id);
                info!("Delete Volume: {:?}, id: {}", file_path, id);
                if file_path.is_empty()
                    || file_path.len() > 255
                    || file_path.contains('\0')
                    || file_path.contains('/')
                {
                    return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
                }
                let status = match self.engine.delete_volume(file_path).await {
                    Ok(()) => 0,
                    Err(e) => {
                        info!(
                            "Delete Volume Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            std::str::from_utf8(path.as_slice()).unwrap(),
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                return Ok((status, 0, 0, 0, Vec::new(), Vec::new()));
            }
            OperationType::CleanVolume => {
                info!("{} Clean Volume", self.engine.group_id);
                info!("Clean Volume: {:?}, id: {}", file_path, id);
                if file_path.is_empty()
                    || file_path.len() > 255
                    || file_path.contains('\0')
                    || file_path.contains('/')
                {
                    return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
                }
                let status = match self.engine.clean_volume(file_path) {
                    Ok(()) => 0,
                    Err(e) => {
                        info!(
                            "Clean Volume Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            std::str::from_utf8(path.as_slice()).unwrap(),
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                return Ok((status, 0, 0, 0, Vec::new(), Vec::new()));
            }
        }
    }
}
