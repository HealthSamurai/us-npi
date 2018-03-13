begin;

create index if not exists org_trgm_idx on organizations using gist ((
'n:' || coalesce((resource#>>'{name}'), '') || ' ' ||
's:' || coalesce((resource#>>'{address,0,state}'), '') || ' ' ||
'c:' || coalesce((resource#>>'{address,0,city}'), '')
) gist_trgm_ops);

commit;
