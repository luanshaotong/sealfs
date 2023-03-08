// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use nix::{
    fcntl::{self, OFlag},
    sys::{
        stat::Mode,
        uio::{pread, pwrite},
    },
};

use crate::server::EngineError;

pub(crate) struct Storage {
    fd: i32,
}

impl Storage {
    pub(crate) fn new(path: &str) -> Storage {
        let oflags = OFlag::O_RDWR;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = fcntl::open(path, oflags, mode);
        match fd {
            Ok(fd) => Self { fd },
            Err(_) => panic!("No Raw blockdevice"),
        }
    }

    pub(crate) fn write(&self, data: &[u8], offset: i64) -> Result<usize, EngineError> {
        match pwrite(self.fd, data, offset) {
            Ok(size) => Ok(size),
            Err(_) => Err(EngineError::IO),
        }
    }

    pub(crate) fn read(&self, size: u32, offset: i64) -> Result<Vec<u8>, EngineError> {
        let mut data = vec![0; size as usize];
        let length = pread(self.fd, data.as_mut_slice(), offset)?;
        Ok(data[..length].to_vec())
    }
}
