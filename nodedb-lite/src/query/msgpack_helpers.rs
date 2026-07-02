//! Low-level MessagePack writer primitives shared by query-layer payload encoders.
//!
//! Engine-internal payloads (cursor + entries for KV scan, sorted scan, columnar
//! materialize, document MERGE results) are framed as MessagePack arrays so the
//! Origin protocol can decode them uniformly. These helpers emit only the wire
//! bytes — no allocation reuse, no error paths — which is why they live as free
//! functions rather than going through a full serializer.

pub(crate) fn write_array_header(out: &mut Vec<u8>, len: usize) {
    if len <= 15 {
        out.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

pub(crate) fn write_bin(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= u8::MAX as usize {
        out.push(0xc4);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xc5);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xc6);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

pub(crate) fn write_str(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= 31 {
        out.push(0xa0 | len as u8);
    } else if len <= u8::MAX as usize {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

pub(crate) fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.push(0xce);
    out.extend_from_slice(&v.to_be_bytes());
}
