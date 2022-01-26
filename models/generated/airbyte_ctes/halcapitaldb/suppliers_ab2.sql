{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('suppliers_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(bankcode as {{ dbt_utils.type_string() }}) as bankcode,
    cast(swiftcode as {{ dbt_utils.type_string() }}) as swiftcode,
    cast(branchcode as {{ dbt_utils.type_string() }}) as branchcode,
    cast(otherdetails as {{ dbt_utils.type_string() }}) as otherdetails,
    cast(suppliercode as {{ dbt_utils.type_string() }}) as suppliercode,
    cast(suppliername as {{ dbt_utils.type_string() }}) as suppliername,
    cast(accountnumber as {{ dbt_utils.type_string() }}) as accountnumber,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('suppliers_ab1') }}
-- suppliers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

