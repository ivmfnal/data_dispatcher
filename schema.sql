drop table project_log;
drop table file_handle_log;
drop table replicas;
drop table file_handles;
drop table files;
drop table projects;

create table projects
(
	id	                bigserial primary key,
	owner	            text,
	created_timestamp   timestamp with time zone     default now(),
    end_timestamp       timestamp with time zone,
    state	            text,
    retry_count         int,
    attributes          jsonb  default '{}'::jsonb
);

create table files
(
    namespace text,
    name text,
    time_added  timestamp with time zone    default now(),
    primary key(namespace, name)
);

create table rses
(
    name            text    primary key,
    description     text    default '',
    is_available    boolean default false,
    is_tape         boolean default false,
    pin_url         text,
    poll_url        text,
    remove_prefix   text    default '',
    add_prefix      text    default '',
    preference      int     default 0
);

create table replicas
(
    namespace   text,
    name        text,
    rse         text        references rses(name) on delete cascade,
    path        text,
    url         text,
    available   boolean     default false,
    preference  int         default 0,
    primary key (namespace, name, rse),
    foreign key (namespace, name) references files (namespace, name)    on delete cascade
);

create index replicas_specs on replicas ((namespace || ':' || name));

create view replicas_with_rse_availability as
    select replicas.*, rses.is_available as rse_available
        from replicas, rses
        where rses.name = replicas.rse
;

create table file_handles
(
    project_id  bigint references projects(id) on delete cascade,
    id          bigserial primary key,
    namespace   text,
    name        text,
    state       text,
    worker_id   text,
    attempts    int default 0,
    attributes  jsonb  default '{}'::jsonb,
    foreign key (namespace, name) references files (namespace, name)
);

create unique index file_handles_project_id_filespec on file_handles(project_id, namespace, name);
create index file_handles_project_id on file_handles(project_id);
create index file_handles_filespec on file_handles(namespace, name);

create table project_log
(
    project_id  bigint  references projects on delete cascade,
    t           timestamp with time zone     default now(),
    type        text,
    data        jsonb,
    primary key (project_id, t)
);

create table file_handle_log
(
    project_id  bigint,
    namespace   text,
    name        text,
    t           timestamp with time zone     default now(),
    type        text,
    data        jsonb,
    primary key (project_id, namespace, name, t),
    foreign key (project_id, namespace, name) references file_handles on delete cascade
);

create table file_log
(
    namespace   text,
    name        text,
    t           timestamp with time zone     default now(),
    type        text,
    data        jsonb,
    primary key (namespace, name, t),
    foreign key (namespace, name) references files(namespace, name) on delete cascade
);

