begin;

-- drop index if exists pract_trgm_idx;

create index if not exists pract_zip_trgm_idx on practitioner
using gist ((resource#>>'{address,0,postalCode}') gist_trgm_ops);

create index if not exists pract_city_trgm_idx on practitioner
using gist ((resource#>>'{address,0,city}') gist_trgm_ops);

create index if not exists pract_state_trgm_idx on practitioner
using gist ((resource#>>'{address,0,state}') gist_trgm_ops);

create index if not exists pract_given_trgm_idx on practitioner
using gist ((coalesce((resource#>>'{name,0,given,0}'), '') || ' '
|| coalesce((resource#>>'{name,0,given,1}'), '') || ' '
|| coalesce((resource#>>'{name,1,given,0}'), '') || ' '
|| coalesce((resource#>>'{name,1,given,1}'), '')) gist_trgm_ops);

create index if not exists pract_family_trgm_idx on practitioner
using gist ((coalesce((resource#>>'{name,0,family}'), '') || ' '
|| coalesce((resource#>>'{name,1,family}'), '')) gist_trgm_ops);

create index if not exists pract_prefix_trgm_idx on practitioner
using gist ((coalesce((resource#>>'{name,0,prefix,0}'), '') || ' '
|| coalesce((resource#>>'{name,0,prefix,1}'), '') || ' '
|| coalesce((resource#>>'{name,1,prefix,0}'), '') || ' '
|| coalesce((resource#>>'{name,1,prefix,1}'), '')) gist_trgm_ops);

create index if not exists pract_suffix_trgm_idx on practitioner
using gist ((coalesce((resource#>>'{name,0,suffix,0}'), '') || ' '
|| coalesce((resource#>>'{name,1,suffix,0}'), '')) gist_trgm_ops);

commit;
