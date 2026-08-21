#!/bin/bash
export CWMS_PASSWORD=`tr -cd '[:alnum:]' < /dev/urandom | fold -w25 | head -n1`
cat > /tmp/create-app-user.sql <<EOF
declare
    user_count integer;
begin
    select count(*) into user_count from dba_users where upper(username)=upper('&1');
    if (user_count = 0) then
        execute immediate 'create user &1 identified by "&2"';
    else
        execute immediate 'alter user &1 identified by "&2"';
    end if;
    -- assign a new CWMS_20 password for new operations
    select count(*) into user_count from dba_users where upper(username)='CWMS_20';
    if (user_count > 0) then
        execute immediate 'alter user cwms_20 identified by "&3"';
    end if;
end;
/
EOF

cat > /tmp/cda-user-info.sql <<EOF
define cda_user='$CDA_USERNAME';
EOF

# Create app user first, one of the migration steps is to update the "cwms user" information for that user

sqlplus $BUILDUSER/$BUILDUSER_PASSWORD@$DB_HOST_PORT$DB_NAME @/tmp/create-app-user "$CDA_USERNAME" "$CDA_PASSWORD" "$CWMS_PASSWORD"

exec $*