begin;

create table organizations (
    id text primary key,
    deleted boolean not null default false,
    resource jsonb null
);

commit;
