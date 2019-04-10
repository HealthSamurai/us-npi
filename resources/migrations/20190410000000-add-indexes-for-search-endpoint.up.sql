BEGIN;

CREATE INDEX idxprcgiven ON practitioner ((resource#>>'{name,0,given}'));
CREATE INDEX idxprcfamily ON practitioner ((resource#>>'{name,0,family}'));
CREATE INDEX idxprcspec ON practitioner ((resource#>>'{qualification,0,code,coding,0,code}'));

CREATE INDEX idxorgname ON organizations ((resource#>>'{name}'));

CREATE INDEX idxprczip ON practitioner ((resource#>>'{address,0,postalCode}'));
CREATE INDEX idxorgzip ON organizations ((resource#>>'{address,0,postalCode}'));

CREATE INDEX idxprccity ON practitioner ((resource#>>'{address,0,city}'));
CREATE INDEX idxorgcity ON organizations ((resource#>>'{address,0,city}'));

CREATE INDEX idxprcstate ON practitioner ((resource#>>'{address,0,state}'));
CREATE INDEX idxorgstate ON organizations ((resource#>>'{address,0,state}'));

COMMIT;
