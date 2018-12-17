begin;

drop index if exists org_trgm_idx;

create index if not exists org_trgm_idx on organizations using gist ((
'n:' || coalesce((resource#>>'{name}'), '') || ' ' ||
's:' || coalesce((resource#>>'{address,0,state}'), '') || ' ' ||
'c:' || coalesce((resource#>>'{address,0,city}'), '') || ' ' ||
'zip:' || coalesce((resource#>>'{address,0,postalCode}'), '')
) gist_trgm_ops);

create index if not exists org_zip_trgm_idx on organizations
using gist ((resource#>>'{address,0,postalCode}') gist_trgm_ops);

create index if not exists org_city_trgm_idx on organizations
using gist ((resource#>>'{address,0,city}') gist_trgm_ops);

create index if not exists org_state_trgm_idx on organizations
using gist ((resource#>>'{address,0,state}') gist_trgm_ops);

create index if not exists org_name_trgm_idx on organizations
using gist ((resource#>>'{name}') gist_trgm_ops);

commit;
