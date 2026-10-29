-- Undo delegation publication storage only when no retained receipts exist.
LOCK TABLE delegation_publications IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed'
    OR EXISTS(SELECT 1 FROM delegation_publications) THEN
  RAISE EXCEPTION 'retained delegation publications require the current reader';
 END IF;
END $$;
DROP TABLE delegation_publications;
DROP FUNCTION preserve_delegation_publication();
