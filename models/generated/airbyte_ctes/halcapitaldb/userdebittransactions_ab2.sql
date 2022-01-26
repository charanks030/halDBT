{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('userdebittransactions_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(amount as {{ dbt_utils.type_float() }}) as amount,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(userid as {{ dbt_utils.type_string() }}) as userid,
    cast(usageid as {{ dbt_utils.type_string() }}) as usageid,
    cast(createdat as {{ dbt_utils.type_timestamp() }}) as createdat,
    cast(updatedat as {{ dbt_utils.type_timestamp() }}) as updatedat,
    cast(supplierid as {{ dbt_utils.type_string() }}) as supplierid,
    cast(transactionid as {{ dbt_utils.type_string() }}) as transactionid,
    cast(usermastercontractid as {{ dbt_utils.type_string() }}) as usermastercontractid,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('userdebittransactions_ab1') }}
-- userdebittransactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

