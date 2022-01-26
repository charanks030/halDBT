{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('usercredits_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(userid as {{ dbt_utils.type_string() }}) as userid,
    cast(lastdate as {{ dbt_utils.type_timestamp() }}) as lastdate,
    cast(createdat as {{ dbt_utils.type_timestamp() }}) as createdat,
    cast(updatedat as {{ dbt_utils.type_timestamp() }}) as updatedat,
    cast(usedamount as {{ dbt_utils.type_float() }}) as usedamount,
    cast(financeamount as {{ dbt_utils.type_float() }}) as financeamount,
    cast(transactionid as {{ dbt_utils.type_string() }}) as transactionid,
    cast(availablebalance as {{ dbt_utils.type_float() }}) as availablebalance,
    cast(creditlimitproductid as {{ dbt_utils.type_string() }}) as creditlimitproductid,
    cast(outstandingAmount as {{ dbt_utils.type_float() }}) as outstandingAmount,
    cast(outstandingamount_aibyte_transform as {{ dbt_utils.type_string() }}) as outstandingamount_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('usercredits_ab1') }}
-- usercredits
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

