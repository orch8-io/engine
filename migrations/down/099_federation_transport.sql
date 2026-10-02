-- Revert the federation transport registry, outbound calls, and region fence.
DROP TABLE IF EXISTS region_fence;
DROP TABLE IF EXISTS federation_calls;
DROP TABLE IF EXISTS federation_peers;
