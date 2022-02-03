use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

pub type Entry = Vec<u8>;

// a wrapper of raw pointer
#[derive(Clone, Copy, Debug)]
pub struct RawPtr<T>(pub(crate) *const T);

impl<T> Default for RawPtr<T> {
    fn default() -> Self {
        Self(0 as *const T)
    }
}

impl<T> DerefMut for RawPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.0 as *mut T) }
    }
}

impl<T> Deref for RawPtr<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.0) }
    }
}

impl<T> RawPtr<T> {
    pub(crate) fn new(v: &T) -> RawPtr<T> {
        RawPtr(v as *const T)
    }
    pub(crate) fn unwrap(&self) -> *const T {
        self.0
    }
}

// pub struct Entry {
//     ptr: *const u8,
//     len: usize,
// }

// impl Entry {
//     pub fn from_slice(s: &[u8]) -> Entry {
//         let ptr = if s.len() > 0 {
//             &s[0] as *const u8
//         } else {
//             std::ptr::null()
//         };
//         Entry { ptr, len: s.len() }
//     }
//     pub fn len(&self) -> usize {
//         self.len
//     }
//     pub fn slice(&self) -> &[u8] {
//         unsafe { from_raw_parts(self.ptr, self.len) }
//     }
// }
