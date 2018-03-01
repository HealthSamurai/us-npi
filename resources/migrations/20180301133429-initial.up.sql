begin;

create table if not exists practitioner (
    id       text primary key,
    deleted  boolean not null default false,
    resource jsonb
);

-- copied from usnpi.npi/trgrm_idx
CREATE INDEX IF NOT EXISTS pract_trgm_idx ON practitioner USING GIST ((
'g:' || coalesce((resource#>>'{name,0,given,0}'), '') || ' ' ||
'g:' || coalesce((resource#>>'{name,0,given,1}'), '') || ' ' ||
'm:' || coalesce((resource#>>'{name,0,middle,0}'), '') || ' ' ||
'p:' || coalesce((resource#>>'{name,0,prefix,0}'), '') || ' ' ||
'z:' || coalesce((resource#>>'{name,0,siffix,0}'), '') || ' ' ||
'f:' || coalesce((resource#>>'{name,0,family}'), '') || ' ' ||
'g:' || coalesce((resource#>>'{name,1,given,0}'), '') || ' ' ||
'g:' || coalesce((resource#>>'{name,1,given,1}'), '') || ' ' ||
'm:' || coalesce((resource#>>'{name,1,middle,0}'), '') || ' ' ||
'p:' || coalesce((resource#>>'{name,1,prefix,0}'), '') || ' ' ||
'z:' || coalesce((resource#>>'{name,1,siffix,0}'), '') || ' ' ||
'f:' || coalesce((resource#>>'{name,1,family}'), '') || ' ' ||
's:' || coalesce((resource#>>'{address,0,state}'), '') || ' ' ||
'c:' || coalesce((resource#>>'{address,0,city}'), '')
) gist_trgm_ops);

commit;
