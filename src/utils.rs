use std::{mem::size_of, slice::from_raw_parts};

pub(crate) unsafe fn struct_to_slice<T: Sized>(p: &T) -> &[u8] {
    from_raw_parts(p as *const T as *const u8, size_of::<T>())
}
pub(crate) unsafe fn arr_to_slice<T: Sized>(p: &[T]) -> &[u8] {
    from_raw_parts(p.as_ptr() as *const u8, size_of::<T>())
}
