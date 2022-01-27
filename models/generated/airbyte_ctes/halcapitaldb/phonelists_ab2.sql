{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('phonelists_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(sign as {{ dbt_utils.type_string() }}) as sign,
    cast(createdat as {{ dbt_utils.type_string() }}) as createdat,
    cast(updatedat as {{ dbt_utils.type_string() }}) as updatedat,
    cast(phonenumber as {{ dbt_utils.type_string() }}) as phonenumber,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('phonelists_ab1') }}
-- phonelists
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

