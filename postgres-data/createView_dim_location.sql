-- Dim Location
-- If they want to relacionate with table fire_incidents, enable the column "incident_number";

CREATE VIEW dim_location as (
	SELECT 
		fi.incident_number, 
		fi.address,
		fi.city,
		fi.zipcode,
		fi.battalion,
		fi.station_area,
		fi.box,
		fi.point_latitude,
		fi.point_longitude,
		fi.supervisor_district,
		fi.neighborhood_district
	FROM 
		public.fire_incidents fi 
)

select * from dim_location