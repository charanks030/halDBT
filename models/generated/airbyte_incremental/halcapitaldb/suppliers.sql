{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "halcapitaldb",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('suppliers_ab3') }}
select
    _id,
    bankcode,
    swiftcode,
    branchcode,
    otherdetails,
    suppliercode,
    suppliername,
    accountnumber,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_suppliers_hashid
from {{ ref('suppliers_ab3') }}
-- suppliers from {{ source('halcapitaldb', '_airbyte_raw_suppliers') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

