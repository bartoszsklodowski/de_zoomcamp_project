
select 
    _LSOA21CD as lsoa_code, 
    LSOA21NM as lsoa_name, 
from {{ ref('lsoa_names_codes') }}

