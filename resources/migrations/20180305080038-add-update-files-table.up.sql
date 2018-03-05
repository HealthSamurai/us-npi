begin;

create table if not exists npi_updates (
    id serial primary key,
    type text not null,
    date timestamp with time zone not null default (current_timestamp at time zone 'UTC'),
    items integer not null default 0,
    url text not null
);

commit;
