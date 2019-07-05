BEGIN;

DROP INDEX IF EXISTS pract_given_sort;
DROP INDEX IF EXISTS org_name_sort;

CREATE INDEX pract_given_sort ON practitioner ((resource#>>'{name,0,family}'));
CREATE INDEX org_name_sort ON organizations ((resource#>>'{name}'));

COMMIT;
