;; The conformance fixture for the `pumper:app@0.1.0` world: the smallest
;; component that is a real dynamic app.
;;
;; Hand-written in the component text format ON PURPOSE. Every other way of
;; producing a component needs a toolchain (`wasm-tools component new`, or a
;; wasip2 target) that CI may not have, and a gate that can only run where a
;; toolchain is installed is a gate that stops running. This one is compiled by
;; `wat::parse_str` from the crate's own dependency tree, so the host's
;; end-to-end path — link, probe describe(), instantiate, call run(), lift the
;; result — is exercised on every `cargo test`.
;;
;; It exports `describe` and `run`, imports nothing, and returns constants:
;; the point is the ABI, not the scraping.
(component
  (core module $m
    (memory (export "memory") 1)

    ;; {"description":"echo app","params_schema":{"type":"object"}}  (60 bytes)
    (data (i32.const 256) "{\"description\":\"echo app\",\"params_schema\":{\"type\":\"object\"}}")
    ;; {"ok":true}  (11 bytes)
    (data (i32.const 512) "{\"ok\":true}")

    ;; Bump allocator over [1024, 64KiB). The host calls this to place the
    ;; params string into guest memory before `run`.
    (global $next (mut i32) (i32.const 1024))
    (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
      (local $ptr i32)
      (local.set $ptr (global.get $next))
      (global.set $next (i32.add (local.get $ptr) (local.get 3)))
      (local.get $ptr))

    ;; describe() -> string: return area at 0 holds (ptr, len).
    (func (export "describe") (result i32)
      (i32.store (i32.const 0) (i32.const 256))
      (i32.store (i32.const 4) (i32.const 60))
      (i32.const 0))

    ;; run(params) -> result<string, string>: return area at 16 holds
    ;; (tag, ptr, len); tag 0 = ok.
    (func (export "run") (param i32 i32) (result i32)
      (i32.store (i32.const 16) (i32.const 0))
      (i32.store (i32.const 20) (i32.const 512))
      (i32.store (i32.const 24) (i32.const 11))
      (i32.const 16))
  )
  (core instance $i (instantiate $m))

  (type $describe-t (func (result string)))
  (func $describe (type $describe-t)
    (canon lift (core func $i "describe")
      (memory $i "memory")
      (realloc (func $i "cabi_realloc"))))
  (export "describe" (func $describe))

  (type $run-t (func (param "params-json" string) (result (result string (error string)))))
  (func $run (type $run-t)
    (canon lift (core func $i "run")
      (memory $i "memory")
      (realloc (func $i "cabi_realloc"))))
  (export "run" (func $run))
)
