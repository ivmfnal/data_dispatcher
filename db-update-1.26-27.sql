
-- get in the data_dispatcher schema.

set search_path = data_dispatcher;

-- in 1.27 we will need the "urls" column (as well as "url") in 
-- the replicas table.

alter table replicas add column urls jsonb default '[]'::jsonb;

