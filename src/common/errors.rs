// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::ffi::CStr;

use libc::strerror;

pub const CONNECTION_ERROR: i32 = 10001;
pub const INVALID_CLUSTER_STATUS: i32 = 10002;
pub const DATABASE_ERROR: i32 = 10003;
pub const SERIALIZATION_ERROR: i32 = 10004;
pub const CHANGE_CLUSTER_WHILE_NOT_IDLE: i32 = 10005;
pub const SERVER_IS_NOT_LEADER: i32 = 10006;
pub const GROUP_MANAGER_ERROR: i32 = 10007;
pub const NEW_HASH_RING_IS_EMPTY: i32 = 10008;


pub fn status_to_string(status: i32) -> String {
    match status {
        CONNECTION_ERROR => "CONNECTION_ERROR".to_string(),
        INVALID_CLUSTER_STATUS => "INVALID_CLUSTER_STATUS".to_string(),
        DATABASE_ERROR => "DATABASE_ERROR".to_string(),
        SERIALIZATION_ERROR => "SERIALIZATION_ERROR".to_string(),
        CHANGE_CLUSTER_WHILE_NOT_IDLE => "CHANGE_CLUSTER_WHILE_NOT_IDLE".to_string(),
        SERVER_IS_NOT_LEADER => "SERVER_IS_NOT_LEADER".to_string(),
        GROUP_MANAGER_ERROR => "GROUP_MANAGER_ERROR".to_string(),
        NEW_HASH_RING_IS_EMPTY => "NEW_HASH_RING_IS_EMPTY".to_string(),
        _ => unsafe { CStr::from_ptr(strerror(status)) }
            .to_str()
            .unwrap()
            .to_string(),
    }
}
