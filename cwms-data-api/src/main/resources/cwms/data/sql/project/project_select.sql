select project.office_id,
       project.location_id as project_id,
       project.COST_YEAR,
       project.federal_cost,
       project.nonfederal_cost,
       project.FEDERAL_OM_COST,
       project.NONFEDERAL_OM_COST,
       project.authorizing_law,
       project.project_owner,
       project.hydropower_description,
       project.sedimentation_description,
       project.downstream_urban_description,
       project.bank_full_capacity_description,
       pumpback.location_id as pump_back_location_id,
       pumpback.p_office_id as pump_back_office_id,
       neargage.location_id as near_gage_location_id,
       neargage.n_office_id as near_gage_office_id,
       project.yield_time_frame_start,
       project.yield_time_frame_end,
       project.project_remarks
from ( select o.office_id as office_id,
              bl.base_location_id
                  ||substr('-', 1, length(pl.sub_location_id))
                  ||pl.sub_location_id as location_id,
              p.COST_YEAR,
              p.federal_cost,
              p.nonfederal_cost,
              p.FEDERAL_OM_COST,
              p.NONFEDERAL_OM_COST,
              p.authorizing_law,
              p.project_owner,
              p.hydropower_description,
              p.sedimentation_description,
              p.downstream_urban_description,
              p.bank_full_capacity_description,
              p.pump_back_location_code,
              p.near_gage_location_code,
              p.yield_time_frame_start,
              p.yield_time_frame_end,
              p.project_remarks
       from cwms_20.cwms_office o,
            cwms_20.at_base_location bl,
            cwms_20.at_physical_location pl,
            cwms_20.at_project p
       where bl.db_office_code = o.office_code
         and pl.base_location_code = bl.base_location_code
         and p.project_location_code = pl.location_code
     ) project
         left outer join
     ( select pl.location_code,
              o.office_id as p_office_id,
              bl.base_location_id
                  ||substr('-', 1, length(pl.sub_location_id))
                  ||pl.sub_location_id as location_id
       from cwms_20.cwms_office o,
            cwms_20.at_base_location bl,
            cwms_20.at_physical_location pl
       where bl.db_office_code = o.office_code
         and pl.base_location_code = bl.base_location_code
     ) pumpback on pumpback.location_code = project.pump_back_location_code
         left outer join
     ( select pl.location_code,
              o.office_id as n_office_id,
              bl.base_location_id
                  ||substr('-', 1, length(pl.sub_location_id))
                  ||pl.sub_location_id as location_id
       from cwms_20.cwms_office o,
            cwms_20.at_base_location bl,
            cwms_20.at_physical_location pl
       where bl.db_office_code = o.office_code
         and pl.base_location_code = bl.base_location_code
     ) neargage on neargage.location_code = project.near_gage_location_code
