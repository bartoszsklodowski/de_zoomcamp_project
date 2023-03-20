with crimes_data as (
    select * from {{ ref('stg_uk_crimes') }}
),

ref_dim_lsoa as (
    select * from {{ ref('dim_lsoa') }}
)

select 
    crimes_data.crimeid, 
    crimes_data.date, 
    crimes_data.reported_by,
    ST_GEOGPOINT(crimes_data.longitude, crimes_data.latitude) as geo_point,
    crimes_data.lsoa_name,
    lsoa_info.lsoa_code,
    crimes_data.location,
    crimes_data.crime_type,
    crimes_data.last_outcome_category, 
from crimes_data
inner join ref_dim_lsoa as lsoa_info
on crimes_data.lsoa_name = lsoa_info.lsoa_name