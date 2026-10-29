-- Remove empty admission tables and hot-spec guards; refuse to erase retained dispatch evidence.
LOCK TABLE worker_task_admissions IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM worker_task_admissions) THEN
  RAISE EXCEPTION 'worker dispatch admissions must be retained';
 END IF;
END $$;
DROP TRIGGER worker_dispatch_spec_update ON worker_tasks;
DROP TRIGGER worker_dispatch_spec_insert ON worker_tasks;
DROP FUNCTION preserve_worker_dispatch_spec();
DROP FUNCTION refuse_worker_dispatch_recreation();
DROP TABLE worker_task_admissions;
