# Authentication and SSO

The standalone engine authenticates workloads with capability-scoped API keys.
It does not implement browser login or OIDC user sessions. For organization SSO,
put the Orch8 Cloud gateway in front of the engine: the gateway terminates OIDC,
maps groups to the six engine roles, issues short-lived tenant-scoped
credentials, and records the user principal in audit metadata.

Do not forward an arbitrary tenant header from an identity proxy. The gateway
must derive tenant and capabilities from the verified identity and overwrite
client-supplied values. Direct engine keys remain appropriate for workers and
automation; rotate them independently from human sessions.
