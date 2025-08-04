begin
cwms_loc.update_location_kind(
                cwms_loc.get_location_code('SPK', 'VALID_LOC_TEST-VALID_LOC'),
                'STREAM',
                'A'
    );

cwms_loc.update_location_kind(
                cwms_loc.get_location_code('SPK', 'VALID_LOC_TEST-VALID_LOC2'),
                'OUTLET',
                'A'
    );

cwms_loc.update_location_kind(
                cwms_loc.get_location_code('SPK', 'VALID_LOC_TEST-VALID_LOC3'),
                'STREAM',
                'A'
    );
end;