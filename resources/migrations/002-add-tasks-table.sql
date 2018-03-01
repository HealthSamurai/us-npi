begin;

create table if not exists tasks (
    id          serial primary key,
    handler     text not null,
    interval    integer not null default 0,
    last_run_at timestamp with time zone null,
    next_run_at timestamp with time zone not null,
    success     boolean not null default false,
    message     text not null default ''
);

commit;
