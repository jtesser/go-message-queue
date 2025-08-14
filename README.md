## module rules: 
1 active message table. priority is irrelevant. 

4 queue tables, 1 per priority.

each topic can have a maximum amount of active messages in a service. 
  service + topic.
  service total max active messages is the sum of all topics.
  
the number of queued messages to add is 1/2 the max active message amount. 
  i.e., max amount = 100; batch size = 50.
  
each priority has a scaling number of concurrently active messages in a service for a topic. 
  highest priority gets most messages and scaling lower. 
  currently scale 40%, 30%, 20%, 10%.
  min number of possible active messages. i.e., guaranteed to run 5 messages if possible. 
  do not fill higher priority with lower priority. This will induce natural throttling. 
  
within a priority we select a random organization to get messages for. 
  within an organization, there is no priority of transactions. Only create date for message. 
  divide total amount allowed across all organizations.
  respects min active messages above.
  
IF random organization does not fill active message amount, get next organization.

## module todo: 
QueueMessage()

GetMessage()

getRandomOrgForPrio()
 get a random organization for a prio to provide GetMessage()
 
pulse()
  check alive status
  
moveDeadMessage()
  move a dead message i.e., from error or no pulse
  
## Example Query to move records 
```
WITH message_rows as (
  DELETE FROM
      queue
  USING (
      SELECT * FROM queue WHERE topic='topic1' AND status = 1 AND owner_organization = 'asdf' order by create_date_time LIMIT 10 FOR UPDATE SKIP LOCKED
  ) q
  WHERE q.sub_transaction_id = queue.sub_transaction_id RETURNING queue.*
)
INSERT INTO running_processes
SELECT * FROM message_rows RETURNING *;
```

## DDL for Base Tables 
```
create table if not exists queue
(
    sub_transaction_id uuid not null primary key,
    owner_organization varchar(36),
    transaction_id     uuid,
    pulse              timestamp,
    create_date_time   timestamp,
    retry_date_time    timestamp,
    topic              varchar(25),
    status             integer,
    retries            integer,
    pulses             integer,
    errors             integer,
    payload            text
);

create index if not exists queue_messagequery_idx
    on queue (topic, status, owner_organization, create_date_time);

create index if not exists queue_pulse_idx
    on queue (pulse);

create index if not exists queue_owner_idx
    on queue (owner_organization, topic);

create index if not exists queue_transaction_id_idx
    on queue using hash (transaction_id);

create index if not exists queue_subtransaction_id_idx
    on queue using hash (sub_transaction_id);

create index if not exists queue_transaction_id_create_idx
    on queue (transaction_id, create_date_time);

create table if not exists running_processes
(
    sub_transaction_id uuid not null primary key,
    owner_organization varchar(36),
    transaction_id     uuid,
    pulse              timestamp,
    create_date_time   timestamp,
    retry_date_time    timestamp,
    topic              varchar(25),
    status             integer,
    retries            integer,
    pulses             integer,
    errors             integer,
    payload            text
);

create index if not exists running_processes_messagequery_idx
    on running_processes (topic, status, owner_organization, create_date_time);

create index if not exists running_processes_pulse_idx
    on running_processes (pulse);

create index if not exists running_processes_transaction_id_idx
    on running_processes using hash (transaction_id);

create index if not exists running_processes_sub_transaction_id_idx
    on running_processes using hash (sub_transaction_id);

create index if not exists running_processes_transaction_create_id_idx
    on running_processes (transaction_id, create_date_time);

```
## Quesry to generate data to test 
```
insert into queue (sub_transaction_id, transaction_id, owner_organization, topic ,status, create_date_time)
    select
        gen_random_uuid(),
        unnest(ARRAY['17c372e6-2ca2-48c8-a756-9aa219e5395b'::uuid, '36ce7a19-3c8f-4298-8b77-c1ab8217aeb9'::uuid, 'c6f76cb5-013d-4922-9e56-28ebbe54b4ee'::uuid, '49a46294-91dc-4259-a5de-5b8a3e1e3c4b'::uuid, 'ea5780d1-13df-4fe4-9821-8908de484c00'::uuid]),
        unnest(ARRAY['companyb','sanity','companya','lake','companyc']),
        unnest(ARRAY['topic1','topic2','topic3','topic4','topic5']),
        random() * 2,
        now() - '20 years'::interval * random()
    from generate_series(1,100000) i;
```
