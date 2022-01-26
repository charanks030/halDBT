{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('usercreditscores_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast({{ adapter.quote('data') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('data') }},
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(userid as {{ dbt_utils.type_string() }}) as userid,
    cast(createdat as {{ dbt_utils.type_timestamp() }}) as createdat,
    cast(createdby as {{ dbt_utils.type_string() }}) as createdby,
    cast(updatedat as {{ dbt_utils.type_timestamp() }}) as updatedat,
    cast(modifiedat as {{ dbt_utils.type_string() }}) as modifiedat,
    cast(modifiedby as {{ dbt_utils.type_string() }}) as modifiedby,
    cast(score as {{ dbt_utils.type_numeric() }}) as score,
    cast(score_aibyte_transform as {{ dbt_utils.type_string() }}) as score_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('usercreditscores_ab1') }}
-- usercreditscores
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

