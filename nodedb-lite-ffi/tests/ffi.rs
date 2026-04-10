use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use nodedb_lite_ffi::*;

#[test]
fn open_close_in_memory() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 1);
        assert!(!handle.is_null());
        nodedb_close(handle);
    }
}

#[test]
fn null_handle_returns_error() {
    unsafe {
        assert_eq!(nodedb_flush(std::ptr::null_mut()), NODEDB_ERR_NULL);
    }
}

#[test]
fn close_null_is_noop() {
    unsafe {
        nodedb_close(std::ptr::null_mut());
    }
}

#[test]
fn vector_insert_and_search() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 1);
        assert!(!handle.is_null());

        let coll = CString::new("vecs").unwrap();
        let id = CString::new("v1").unwrap();
        let emb = [1.0f32, 0.0, 0.0];

        let rc = nodedb_vector_insert(handle, coll.as_ptr(), id.as_ptr(), emb.as_ptr(), 3);
        assert_eq!(rc, NODEDB_OK);

        let query = [1.0f32, 0.0, 0.0];
        let mut out: *mut c_char = std::ptr::null_mut();
        let rc = nodedb_vector_search(handle, coll.as_ptr(), query.as_ptr(), 3, 5, &mut out);
        assert_eq!(rc, NODEDB_OK);
        assert!(!out.is_null());

        let json = CStr::from_ptr(out).to_str().unwrap();
        assert!(json.contains("v1"));
        nodedb_free_string(out);

        nodedb_close(handle);
    }
}

#[test]
fn graph_insert_and_traverse() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 1);

        let from = CString::new("alice").unwrap();
        let to = CString::new("bob").unwrap();
        let label = CString::new("KNOWS").unwrap();

        let rc = nodedb_graph_insert_edge(handle, from.as_ptr(), to.as_ptr(), label.as_ptr());
        assert_eq!(rc, NODEDB_OK);

        let mut out: *mut c_char = std::ptr::null_mut();
        let rc = nodedb_graph_traverse(handle, from.as_ptr(), 2, &mut out);
        assert_eq!(rc, NODEDB_OK);
        assert!(!out.is_null());

        let json = CStr::from_ptr(out).to_str().unwrap();
        assert!(json.contains("alice"));
        assert!(json.contains("bob"));
        nodedb_free_string(out);

        nodedb_close(handle);
    }
}

#[test]
fn document_crud_via_ffi() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 1);

        let coll = CString::new("notes").unwrap();
        let body = CString::new(r#"{"id":"n1","fields":{"title":{"String":"Hello"}}}"#).unwrap();

        let rc = nodedb_document_put(handle, coll.as_ptr(), body.as_ptr(), std::ptr::null_mut());
        assert_eq!(rc, NODEDB_OK);

        let id = CString::new("n1").unwrap();
        let mut out: *mut c_char = std::ptr::null_mut();
        let rc = nodedb_document_get(handle, coll.as_ptr(), id.as_ptr(), &mut out);
        assert_eq!(rc, NODEDB_OK);
        assert!(!out.is_null());

        let json = CStr::from_ptr(out).to_str().unwrap();
        assert!(json.contains("n1"));
        nodedb_free_string(out);

        let rc = nodedb_document_delete(handle, coll.as_ptr(), id.as_ptr());
        assert_eq!(rc, NODEDB_OK);

        let rc = nodedb_document_get(handle, coll.as_ptr(), id.as_ptr(), &mut out);
        assert_eq!(rc, NODEDB_ERR_NOT_FOUND);

        nodedb_close(handle);
    }
}

#[test]
fn free_null_string_is_noop() {
    unsafe {
        nodedb_free_string(std::ptr::null_mut());
    }
}
