-- Revert durable webhook nonce replay protection.
DROP TABLE IF EXISTS webhook_replay_nonces;
