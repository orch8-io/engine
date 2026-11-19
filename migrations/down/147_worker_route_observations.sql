-- Remove persisted worker route observations and the registration route index.
DROP INDEX idx_worker_registrations_route;
DROP TABLE worker_route_observations;
