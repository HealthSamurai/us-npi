begin;

create table if not exists organizations (
    id text primary key,
    deleted boolean not null default false,
    resource jsonb null
);

commit;
