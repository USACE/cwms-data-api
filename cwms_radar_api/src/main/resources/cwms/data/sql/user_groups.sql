SELECT unique user_group_id          
     FROM    (SELECT username,
                     db_office_id,
                     user_group_id,
                     user_group_desc,
                     db_office_code db_office_code,
                     user_group_code,
                     CASE
                        WHEN ROWIDTOCHAR (b.ROWID) IS NOT NULL THEN 'T'
                        ELSE 'F'
                     END
                        is_member
                FROM    (SELECT a.userid username,
                                c.office_id db_office_id,
                                b.user_group_id,
                                b.user_group_desc,
                                b.db_office_code,
                                b.user_group_code,
                                'T' is_member
                           FROM cwms_20.at_sec_cwms_users a,
                                cwms_20.at_sec_user_groups b,
                                cwms_20.cwms_office c
                          WHERE b.db_office_code = c.office_code) a
                          
                     LEFT OUTER JOIN
                        cwms_20.at_sec_users b
                     USING (username, db_office_code, user_group_code)) a                
          LEFT OUTER JOIN
             cwms_20.at_sec_locked_users
          USING (username, db_office_code)
where is_member ='T' and username=?