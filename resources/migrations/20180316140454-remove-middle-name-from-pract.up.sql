begin;

drop index if exists pract_trgm_idx;

create index if not exists pract_trgm_idx on practitioner using gist ((
'g:' || coalesce((resource#>>'{name,0,given,0}'), '') || ' ' ||
'g:' || coalesce((resource#>>'{name,0,given,1}'), '') || ' ' ||
'p:' || coalesce((resource#>>'{name,0,prefix,0}'), '') || ' ' ||
'z:' || coalesce((resource#>>'{name,0,siffix,0}'), '') || ' ' ||
'f:' || coalesce((resource#>>'{name,0,family}'), '') || ' ' ||
'g:' || coalesce((resource#>>'{name,1,given,0}'), '') || ' ' ||
'g:' || coalesce((resource#>>'{name,1,given,1}'), '') || ' ' ||
'p:' || coalesce((resource#>>'{name,1,prefix,0}'), '') || ' ' ||
'z:' || coalesce((resource#>>'{name,1,suffix,0}'), '') || ' ' ||
'f:' || coalesce((resource#>>'{name,1,family}'), '') || ' ' ||
's:' || coalesce((resource#>>'{address,0,state}'), '') || ' ' ||
'zip:' || coalesce((resource#>>'{address,0,postalCode}'), '') || ' ' ||
'c:' || coalesce((resource#>>'{address,0,city}'), '')
) gist_trgm_ops);

commit;
