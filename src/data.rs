use std::slice::from_raw_parts;

pub struct Entry {
    ptr: *const u8,
    len: usize,
}

impl Entry {
    pub fn from_slice(s: &[u8]) -> Entry {
        let ptr = if s.len() > 0 {
            &s[0] as *const u8
        } else {
            std::ptr::null()
        };
        Entry { ptr, len: s.len() }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr, self.len) }
    }
}
