BEGIN;

DROP INDEX IF EXISTS idxorgspec;

CREATE INDEX idxorgspec ON organizations ((resource#>>'{type,0,coding,0,code}'));

COMMIT;
