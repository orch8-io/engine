use std::{env, time::Duration};

use orch8_engine::remote_worker::delegation_receiver::EchoDelegationReceiver;
use orch8_types::{
    continuity::RuntimeId,
    ids::{SequenceId, TenantId},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let [api_base, tenant, runtime, sequence]: [String; 4] = env::args().skip(1).collect::<Vec<_>>().try_into()
        .map_err(|_| "usage: phone_delegation_receiver <api-base> <tenant> <runtime UUID> <sequence UUID>")?;
    let mut receiver = EchoDelegationReceiver::new(
        &api_base,
        TenantId::new(tenant)?,
        RuntimeId::from_uuid(runtime.parse()?),
        SequenceId::from_uuid(sequence.parse()?),
        env::var("ORCH8_WORKER_KEY")?,
    )?;
    receiver.register().await?;
    println!("Registered; waiting for a phone delegation");
    loop {
        match receiver.receive_once().await? {
            Some(task) => println!("Backend acknowledged task {task}"),
            None => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}
