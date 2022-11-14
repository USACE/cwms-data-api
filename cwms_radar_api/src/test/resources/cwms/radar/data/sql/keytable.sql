create table cwms_20.apikeys(
    key varchar2(300) not null primary key,
    username varchar2(16) not null references cwms_20.at_sec_cwms_users (userid)
)
