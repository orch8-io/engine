fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // Ubuntu 22.04 ships protoc 3.12, where proto3 optional fields still
        // require the compatibility flag. Newer protoc versions accept it too.
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["../proto/orch8.proto"], &["../proto"])?;
    Ok(())
}
