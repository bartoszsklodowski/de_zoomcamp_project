with tripdata as 
(
  select *,
    row_number() over(partition by Crime_ID) as rn
  from {{ source('staging','uk_crimes_data_all') }}
)
select
    -- identifiers
    Crime_ID as crimeid,

    -- date
    cast(Month || '-01' as date) as date,
    
    -- crime info
    Reported_by as reported_by,
    Longitude as longitude,
    Latitude as latitude,
    Location as location,
    LSOA_name as lsoa_name,
    Crime_type as crime_type,
    Last_outcome_category as last_outcome_category
from tripdata
where rn = 1

