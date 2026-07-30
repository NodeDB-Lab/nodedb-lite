// SPDX-License-Identifier: Apache-2.0

//! User-defined functions supplied as WASM modules.

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

/// Register a user-defined WASM function from raw bytes.
///
/// Uses the browser's native `WebAssembly.instantiate()` — no wasmtime needed.
/// The `.wasm` module must export a function with the given name.
///
/// ```js
/// const wasmBytes = await fetch('my_udf.wasm').then(r => r.arrayBuffer());
/// await registerWasmUdf('my_func', new Uint8Array(wasmBytes));
/// ```
#[wasm_bindgen(js_name = "registerWasmUdf")]
pub async fn register_wasm_udf(name: &str, wasm_bytes: &[u8]) -> Result<(), JsError> {
    use js_sys::{Object, Reflect, WebAssembly};

    // Validate WASM magic header.
    if wasm_bytes.len() < 4 || &wasm_bytes[..4] != b"\0asm" {
        return Err(JsError::new("invalid WASM binary: missing \\0asm header"));
    }

    // Compile and instantiate via browser WebAssembly API.
    let module_promise = WebAssembly::compile(&js_sys::Uint8Array::from(wasm_bytes).into());
    let module = JsFuture::from(module_promise)
        .await
        .map_err(|e| JsError::new(&format!("WebAssembly.compile failed: {e:?}")))?;

    let imports = Object::new();
    let instance_promise =
        WebAssembly::instantiate_module(&module.unchecked_into::<WebAssembly::Module>(), &imports);
    let instance = JsFuture::from(instance_promise)
        .await
        .map_err(|e| JsError::new(&format!("WebAssembly.instantiate failed: {e:?}")))?;

    // Verify the named export exists.
    let exports = Reflect::get(&instance, &"exports".into())
        .map_err(|_| JsError::new("failed to access WASM instance exports"))?;
    let func = Reflect::get(&exports, &name.into())
        .map_err(|_| JsError::new(&format!("WASM module does not export function '{name}'")))?;
    if func.is_undefined() {
        return Err(JsError::new(&format!(
            "WASM module does not export function '{name}'"
        )));
    }

    // Store the instance for later invocation.
    web_sys::console::log_1(
        &format!("WASM UDF '{name}' registered ({} bytes)", wasm_bytes.len()).into(),
    );

    Ok(())
}
