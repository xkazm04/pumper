//! A hostile/buggy plugin that never returns. Under the host's fuel budget it
//! traps ("all fuel consumed") instead of hanging the process — the guarantee
//! Python's in-process code execution cannot make.

#[no_mangle]
pub extern "C" fn alloc(len: u32) -> u32 {
    let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
    let ptr = buf.as_mut_ptr() as u32;
    std::mem::forget(buf);
    ptr
}

#[no_mangle]
pub extern "C" fn extract(_ptr: u32, _len: u32) -> u64 {
    // black_box prevents the optimizer from removing the (effectively infinite)
    // loop; fuel runs out long before this counter wraps.
    let mut x: u64 = 0;
    loop {
        x = std::hint::black_box(x.wrapping_add(1));
        if std::hint::black_box(x) == u64::MAX {
            return 0;
        }
    }
}
