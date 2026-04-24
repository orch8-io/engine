#![allow(
    clippy::result_large_err,
    clippy::redundant_closure,
    clippy::items_after_statements
)]

pub mod auth;
pub mod service;

pub mod proto {
    #![allow(
        clippy::default_constructed_unit_structs,
        clippy::too_many_lines,
        clippy::doc_markdown,
        clippy::needless_borrows_for_generic_args,
        clippy::default_trait_access,
        clippy::derive_partial_eq_without_eq,
        clippy::missing_const_for_fn
    )]
    tonic::include_proto!("orch8");
}

pub use proto::orch8_service_server::{Orch8Service, Orch8ServiceServer};
