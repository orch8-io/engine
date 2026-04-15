pub mod service;

pub mod proto {
    tonic::include_proto!("orch8");
}

pub use proto::orch8_service_server::{Orch8Service, Orch8ServiceServer};
