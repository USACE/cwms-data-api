/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

begin
  begin
    cwms_sec.create_user('&webuser','&password',NULL,NULL);
  exception
    when dup_val_on_index then null; -- user already exists
  end;
  
  cwms_sec.add_user_to_group('&webuser','All Users', 'HQ');
  cwms_sec.add_user_to_group('&webuser','CWMS Users', 'HQ');
  cwms_sec.add_user_to_group('&webuser','CWMS PD Users', 'HQ');
  cwms_sec.add_user_to_group('&webuser','CWMS DBA Users', 'HQ');
  cwms_sec.add_user_to_group('&webuser','All Users', 'SPK');
  cwms_sec.add_user_to_group('&webuser','CWMS Users', 'SPK');
  cwms_sec.add_user_to_group('&webuser','CWMS PD Users', 'SPK');
  cwms_sec.add_user_to_group('&webuser','CWMS DBA Users', 'SPK');
  cwms_sec.add_user_to_group('&webuser','All Users', 'SWT');
  cwms_sec.add_user_to_group('&webuser','CWMS Users', 'SWT');
  cwms_sec.add_user_to_group('&webuser','CWMS PD Users', 'SWT');
  cwms_sec.add_user_to_group('&webuser','CWMS DBA Users', 'SWT');
  cwms_sec.add_user_to_group('&user','All Users', 'HQ');
  cwms_sec.add_user_to_group('&user','CWMS Users', 'HQ');
  cwms_sec.add_user_to_group('&user','CWMS PD Users', 'HQ');
  cwms_sec.add_user_to_group('&user','CWMS DBA Users', 'HQ');
  cwms_sec.add_user_to_group('&user','All Users', 'SPK');
  cwms_sec.add_user_to_group('&user','CWMS Users', 'SPK');
  cwms_sec.add_user_to_group('&user','CWMS PD Users', 'SPK');
  cwms_sec.add_user_to_group('&user','CWMS DBA Users', 'SPK');
  

  /** Add a couple of districts*/
  begin
    cwms_sec.add_cwms_user('l2hectest',NULL,'SPK');    
    cwms_sec.update_edipi('l2hectest',1234567890);
    cwms_sec.add_user_to_group('l2hectest','CWMS Users','SPK');
    cwms_sec.add_user_to_group('l2hectest','TS ID Creator','SPK');

    begin
        cwms_sec.create_user('l2hectest_vt','l2hectestvt',NULL,'SPK');
    exception
        when dup_val_on_index then null; -- user already exists
    end;
    cwms_sec.add_cwms_user('l2hectest_vt',NULL,'SPK');
    cwms_sec.update_edipi('l2hectest_vt',2345678901);
    cwms_sec.add_user_to_group('l2hectest_vt','All Users', 'HQ');
    cwms_sec.add_user_to_group('l2hectest_vt','CWMS Users', 'HQ');
    cwms_sec.add_user_to_group('l2hectest_vt','CWMS PD Users', 'HQ');
    cwms_sec.add_user_to_group('l2hectest_vt','CWMS DBA Users', 'HQ');
    cwms_sec.add_user_to_group('l2hectest_vt','All Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest_vt','CWMS Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest_vt','CWMS PD Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest_vt','CWMS DBA Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest_vt','TS ID Creator','SPK');

    cwms_sec.add_cwms_user('m5hectest', NULL, 'SWT');
    cwms_sec.add_user_to_group('m5hectest','CWMS Users','SWT');
    cwms_sec.add_user_to_group('m5hectest','TS ID Creator','SWT');
  exception
    when dup_val_on_index then null; -- user already exists
  end;

  begin
    cwms_sec.add_cwms_user('user2',NULL,'SPK');
    --insert into at_api_keys(userid,key_name,apikey) values('USER2','USER test key','User2key');
  exception
    when dup_val_on_index then null; -- user already exists
  end;
end;
