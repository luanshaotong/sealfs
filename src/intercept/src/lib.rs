use crossbeam_channel::{bounded, Receiver, Sender};
use lazy_static::lazy_static;
use libc::{
    c_char, c_int, c_long, iovec, SYS_close, SYS_creat, SYS_fstat, SYS_ftruncate, SYS_getdents64,
    SYS_lseek, SYS_mkdir, SYS_mkdirat, SYS_open, SYS_openat, SYS_pread64, SYS_preadv, SYS_pwrite64,
    SYS_pwritev, SYS_read, SYS_readv, SYS_rename, SYS_renameat, SYS_rmdir, SYS_stat, SYS_truncate,
    SYS_unlink, SYS_write, SYS_writev, SEEK_CUR, SEEK_END, SEEK_SET,
};
use std::collections::HashMap;
use std::env;
use std::ffi::CStr;
use std::sync::RwLock;

const STAT_SIZE: usize = 144;

#[link(name = "syscall_intercept")]
extern "C" {
    static mut intercept_hook_point:
        extern "C" fn(c_long, c_long, c_long, c_long, c_long, c_long, c_long, *mut c_long) -> c_int;
}

extern "C" fn initialize() {
    unsafe {
        intercept_hook_point = dispatch;
    }
}

/* There is no __attribute__((constructor)) in rust,
 * it is implemented through .init_array */
#[link_section = ".init_array"]
pub static INITIALIZE_CTOR: extern "C" fn() = self::initialize;

#[derive(PartialEq)]
enum Type {
    File,
    Dir,
}

struct FdAttr {
    pathname: String,
    r#type: Type,
    offset: i64,
    size: i64, // maybe consistency problem
}

lazy_static! {
    static ref IDLE_FD: (Sender<c_int>, Receiver<c_int>) = {
        let (s, r) = bounded(1024);
        for i in 10000..11024 {
            s.send(i).unwrap();
        }
        (s, r)
    };
    static ref MOUNT_POINT: String = {
        let mut value = env::var("SEALFS_MOUNT_POINT").unwrap_or("/mnt/sealfs".to_string());
        if value.ends_with('/') {
            value.pop();
        }
        value
    };
    static ref FD_TB: RwLock<HashMap<c_int, FdAttr>> = RwLock::new(HashMap::new());
}

fn get_remotepath(dir: &str, path: &str) -> Result<String, ()> {
    let rpos = match path.rfind('/') {
        Some(value) => value,
        None => return Err(()),
    };
    let path = &path[..=rpos];
    let absolute_path = match path.starts_with("/") {
        true => match std::fs::canonicalize(path) {
            Ok(value) => value,
            Err(_) => return Err(()),
        },
        false => match std::fs::canonicalize(dir) {
            Ok(mut value) => {
                value.push(path);
                value
            }
            Err(_) => return Err(()),
        },
    };
    if !absolute_path.starts_with(MOUNT_POINT.clone()) {
        return Err(());
    }
    Ok(absolute_path.to_str().unwrap()[MOUNT_POINT.len()..].to_string())
}

#[allow(non_upper_case_globals)]
#[no_mangle]
extern "C" fn dispatch(
    syscall_number: c_long,
    arg0: c_long,
    arg1: c_long,
    arg2: c_long,
    arg3: c_long,
    _arg4: c_long,
    _arg5: c_long,
    result: *mut c_long,
) -> c_int {
    match syscall_number {
        SYS_close => {
            if !FD_TB.read().unwrap().contains_key(&(arg0 as c_int)) {
                return 1;
            }
            unsafe {
                *result = close(arg0 as c_int) as c_long;
            }
            return 0;
        }
        // int creat(const char *pathname, mode_t mode)
        SYS_creat => {
            let pathname = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath("", pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };
            unsafe {
                *result = creat(&remote_pathname, arg1 as u32) as c_long;
            }
            return 0;
        }
        // int open(const char *pathname, int flags, mode_t mode)
        SYS_open => {
            let pathname = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath("", pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = open(&remote_pathname, arg1 as i32, arg2 as u32) as c_long;
            }
            return 0;
        }
        // int openat(int dirfd, const char *pathname, int flags, mode_t mode)
        SYS_openat => {
            let dir = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::File {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let pathname = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath(&dir, pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };
            unsafe {
                *result = open(&remote_pathname, arg2 as i32, arg3 as u32) as c_long;
            }
            return 0;
        }
        // int rename(const char *oldpath, const char *newpath)
        SYS_rename => {
            // todo other state
            let oldpath = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let newpath = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let remote_oldpath = match get_remotepath("", oldpath) {
                Ok(value) => value,
                Err(_) => return 1,
            };
            let remote_newpath = match get_remotepath("", newpath) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = rename(&remote_oldpath, &remote_newpath) as c_long;
            }
            return 0;
        }
        // int renameat(int olddirfd, const char *oldpath,
        //             int newdirfd, const char *newpath)
        SYS_renameat => {
            // todo other state
            let olddir = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::File {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let newdir = match FD_TB.read().unwrap().get(&(arg2 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::File {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let oldpath = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let newpath = unsafe { CStr::from_ptr(arg3 as *const c_char).to_str().unwrap() };
            let remote_oldpath = match get_remotepath(&olddir, oldpath) {
                Ok(value) => value,
                Err(_) => return 1,
            };
            let remote_newpath = match get_remotepath(&newdir, newpath) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = rename(&remote_oldpath, &remote_newpath) as c_long;
            }
            return 0;
        }
        // int truncate(const char *path, off_t length)
        SYS_truncate => {
            let pathname = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath("", pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = truncate(&remote_pathname, arg1) as c_long;
            }
            return 0;
        }
        // int ftruncate(int fd, off_t length)
        SYS_ftruncate => {
            let pathname = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::Dir {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let remote_pathname = match get_remotepath("", &pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = truncate(&remote_pathname, arg1) as c_long;
            }
            return 0;
        }
        // int mkdir(const char *pathname, mode_t mode)
        SYS_mkdir => {
            let pathname = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath("", pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = mkdir(&remote_pathname, arg1 as u32) as c_long;
            }
            return 0;
        }
        // int mkdirat(int dirfd, const char *pathname, mode_t mode)
        SYS_mkdirat => {
            let dir = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::File {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let pathname = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath(&dir, pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = mkdir(&remote_pathname, arg2 as u32) as c_long;
            }
            return 0;
        }
        // int rmdir(const char *pathname)
        SYS_rmdir => {
            let pathname = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath("", pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = rmdir(&remote_pathname) as c_long;
            }
            return 0;
        }
        // ssize_t getdents64(int fd, void *dirp, size_t count);
        SYS_getdents64 => {
            let pathname = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::File {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let dirp = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };

            unsafe {
                *result = getdents64(&pathname, dirp) as c_long;
            }
            return 0;
        }
        // int unlink(const char *pathname)
        SYS_unlink => {
            let pathname = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath("", pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            unsafe {
                *result = unlink(&remote_pathname) as c_long;
            }
            return 0;
        }
        //    int stat(const char *restrict pathname,
        //             struct stat *restrict statbuf);
        SYS_stat => {
            let pathname = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let remote_pathname = match get_remotepath("", pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            let statbuf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, STAT_SIZE) };
            unsafe {
                *result = stat(&remote_pathname, statbuf) as c_long;
            }
            return 0;
        }
        // int fstat(int fd, struct stat *statbuf);
        SYS_fstat => {
            let pathname = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => attr.pathname.clone(),
                _ => return 1,
            };
            let remote_pathname = match get_remotepath("", &pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            let statbuf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, STAT_SIZE) };
            unsafe {
                *result = stat(&remote_pathname, statbuf) as c_long;
            }
            return 0;
        }
        // ssize_t read(int fd, void *buf, size_t count);
        SYS_read => {
            match FD_TB.write().unwrap().get_mut(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::Dir {
                        return 1;
                    }
                    let remote_pathname = match get_remotepath("", &attr.pathname) {
                        Ok(value) => value,
                        Err(_) => return 1,
                    };
                    let buf =
                        unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };
                    pread(&remote_pathname, buf, attr.offset);
                }
                _ => return 1,
            };

            return 0;
        }
        // ssize_t pread(int fd, void *buf, size_t count, off_t offset)
        SYS_pread64 => {
            let pathname = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::Dir {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let remote_pathname = match get_remotepath("", &pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            let buf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };
            unsafe {
                *result = pread(&remote_pathname, buf, arg2 as i64) as c_long;
            }
            return 0;
        }
        // ssize_t readv(int fd, const struct iovec *iov, int iovcnt);
        SYS_readv => match FD_TB.write().unwrap().get_mut(&(arg0 as c_int)) {
            Some(attr) => {
                if attr.r#type == Type::Dir {
                    return 1;
                }
                let remote_pathname = match get_remotepath("", &attr.pathname) {
                    Ok(value) => value,
                    Err(_) => return 1,
                };

                let iov =
                    unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
                unsafe {
                    *result = preadv(&remote_pathname, iov, attr.offset) as c_long;
                    attr.offset += *result;
                }
                return 0;
            }
            _ => return 1,
        },
        // ssize_t preadv(int fd, const struct iovec *iov, int iovcnt,
        //                    off_t offset);
        SYS_preadv => {
            let pathname = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::Dir {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let remote_pathname = match get_remotepath("", &pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            let iov = unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
            unsafe {
                *result = preadv(&remote_pathname, iov, arg2 as i64) as c_long;
            }
            return 0;
        }
        // ssize_t write(int fd, const void *buf, size_t count);
        SYS_write => match FD_TB.write().unwrap().get_mut(&(arg0 as c_int)) {
            Some(attr) => {
                if attr.r#type == Type::Dir {
                    return 1;
                }
                let remote_pathname = match get_remotepath("", &attr.pathname) {
                    Ok(value) => value,
                    Err(_) => return 1,
                };

                let buf = unsafe { std::slice::from_raw_parts(arg1 as *const u8, arg2 as usize) };
                unsafe {
                    *result = pwrite(&remote_pathname, buf, attr.offset) as c_long;
                    attr.offset += *result;
                }
                return 0;
            }
            _ => return 1,
        },
        // ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
        SYS_pwrite64 => {
            let pathname = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::Dir {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let remote_pathname = match get_remotepath("", &pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            let buf = unsafe { std::slice::from_raw_parts(arg1 as *const u8, arg2 as usize) };
            unsafe {
                *result = pwrite(&remote_pathname, buf, arg2 as i64) as c_long;
            }
            return 0;
        }
        // ssize_t writev(int fd, const struct iovec *iov, int iovcnt);
        SYS_writev => match FD_TB.write().unwrap().get_mut(&(arg0 as c_int)) {
            Some(attr) => {
                if attr.r#type == Type::Dir {
                    return 1;
                }
                let remote_pathname = match get_remotepath("", &attr.pathname) {
                    Ok(value) => value,
                    Err(_) => return 1,
                };

                let iov =
                    unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
                unsafe {
                    *result = pwritev(&remote_pathname, iov, attr.offset) as c_long;
                    attr.offset += *result;
                }
                return 0;
            }
            _ => return 1,
        },
        // ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt,
        //                    off_t offset);
        SYS_pwritev => {
            let pathname = match FD_TB.read().unwrap().get(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::Dir {
                        return 1;
                    }
                    attr.pathname.clone()
                }
                _ => return 1,
            };
            let remote_pathname = match get_remotepath("", &pathname) {
                Ok(value) => value,
                Err(_) => return 1,
            };

            let iov = unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
            unsafe {
                *result = pwritev(&remote_pathname, iov, arg2 as i64) as c_long;
            }
            return 0;
        }
        // off_t lseek(int fd, off_t offset, int whence);
        SYS_lseek => {
            match FD_TB.write().unwrap().get_mut(&(arg0 as c_int)) {
                Some(attr) => {
                    if attr.r#type == Type::Dir {
                        return 1;
                    }
                    match arg2 as i32 {
                        SEEK_SET => attr.offset = arg1 as i64,
                        SEEK_CUR => attr.offset += arg1 as i64,
                        SEEK_END => attr.offset = attr.size + arg1 as i64,
                        _ => {}
                    };
                }
                _ => return 1,
            };
            return 0;
        }
        _ => return 1,
    };
}

fn close(_fd: c_int) -> c_int {
    todo!()
}

fn creat(_pathname: &str, _mode: u32) -> i32 {
    todo!()
}

fn open(_pathname: &str, _flags: i32, _mode: u32) -> i32 {
    todo!();
}

fn rename(_oldpath: &str, _newpath: &str) -> i32 {
    todo!()
}

fn truncate(_pathname: &str, _length: i64) -> i32 {
    todo!()
}

fn mkdir(_pathname: &str, _mode: u32) -> i32 {
    todo!();
}

fn rmdir(_pathname: &str) -> i32 {
    todo!();
}

fn getdents64(_pathname: &str, _dirp: &mut [u8]) -> isize {
    todo!();
}

fn unlink(_pathname: &str) -> i32 {
    todo!()
}

fn stat(_pathname: &str, _statbuf: &mut [u8]) -> i32 {
    todo!()
}

fn pread(_pathname: &str, _buf: &mut [u8], _offset: i64) -> isize {
    todo!()
}

fn preadv(_pathname: &str, _buf: &[iovec], _offset: i64) -> isize {
    todo!()
}

fn pwrite(_pathname: &str, _buf: &[u8], _offset: i64) -> isize {
    todo!()
}

fn pwritev(_pathname: &str, _buf: &[iovec], _offset: i64) -> isize {
    todo!()
}