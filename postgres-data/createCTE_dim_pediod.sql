-- Dim Period
-- If they want to relational with table fire_incidents, enable the column "incident_number". Otherwise, disable it;
-- It's possible to get more information from calendar with a join between this view and the table "dim_calendar";
-- JOIN: SUBSTRING(dim_pediod.incident_date::VARCHAR,0,11) == dim_calendar.date_full
--select * from dim_calendar

WITH dim_pediod AS (
	SELECT 
		fi.incident_number,
		fi.incident_date,
		fi.alarm_dttm,
		fi.arrival_dttm,
		fi.close_dttm
	FROM 
		public.fire_incidents fi
)		

select * from dim_pediod