BEGIN;

DROP INDEX IF EXISTS idxprcgiven;
DROP INDEX IF EXISTS idxprcfamily;
DROP INDEX IF EXISTS idxprcspec;
DROP INDEX IF EXISTS idxorgname;
DROP INDEX IF EXISTS idxprczip;
DROP INDEX IF EXISTS idxorgzip;
DROP INDEX IF EXISTS idxprccity;
DROP INDEX IF EXISTS idxorgcity;
DROP INDEX IF EXISTS idxprcstate;
DROP INDEX IF EXISTS idxorgstate;

CREATE INDEX idxprcgiven ON practitioner USING gin ((resource#>>'{name,0,given}') gin_trgm_ops);
CREATE INDEX idxprcfamily ON practitioner USING gin ((resource#>>'{name,0,family}') gin_trgm_ops);
CREATE INDEX idxprcspec ON practitioner ((resource#>>'{qualification,0,code,coding,0,code}'));

CREATE INDEX idxorgname ON organizations USING gin ((resource#>>'{name}') gin_trgm_ops);

CREATE INDEX idxprczip ON practitioner ((resource#>>'{address,0,postalCode}'));
CREATE INDEX idxorgzip ON organizations ((resource#>>'{address,0,postalCode}'));

CREATE INDEX idxprccity ON practitioner USING gin ((resource#>>'{address,0,city}') gin_trgm_ops);
CREATE INDEX idxorgcity ON organizations USING gin ((resource#>>'{address,0,city}') gin_trgm_ops);

CREATE INDEX idxprcstate ON practitioner ((resource#>>'{address,0,state}'));
CREATE INDEX idxorgstate ON organizations ((resource#>>'{address,0,state}'));

COMMIT;
