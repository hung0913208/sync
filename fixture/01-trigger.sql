create or replace function notify_message()
returns trigger as $$
declare
  payload JSON;
begin
	payload = json_build_object(
		'action', TG_OP,
		'schema', TG_TABLE_SCHEMA,
		'table', TG_TABLE_NAME,
		'new', row_to_json(NEW),
		'old', row_to_json(OLD)
        );
   	perform pg_notify('event_channel', payload::text);
   	return null;
end;
$$ language plpgsql;
