-- Tạo một procedure
create or replace function notify_message()
returns trigger as $$
declare
  payload JSON;
begin
   payload = json_build_object( 
          'action', TG_OP, 
          'data', row_to_json( NEW ) 
        );
   perform pg_notify( 'event_channel', payload::text );       
   return null;
end;
$$ language plpgsql;

-- Tạo trigger mỗi khi insert or update thì call procedure
