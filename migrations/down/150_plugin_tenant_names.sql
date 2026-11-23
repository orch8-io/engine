-- Fail if different tenants now use the same alias; never delete their rows.
DROP INDEX IF EXISTS idx_plugins_name;
ALTER TABLE plugins DROP CONSTRAINT plugins_pkey, ADD PRIMARY KEY (name);
