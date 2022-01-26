{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('halcapitaldb', '_airbyte_raw_suppliers') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['bankCode'], ['bankCode']) }} as bankcode,
    {{ json_extract_scalar('_airbyte_data', ['swiftCode'], ['swiftCode']) }} as swiftcode,
    {{ json_extract_scalar('_airbyte_data', ['branchCode'], ['branchCode']) }} as branchcode,
    {{ json_extract_scalar('_airbyte_data', ['otherDetails'], ['otherDetails']) }} as otherdetails,
    {{ json_extract_scalar('_airbyte_data', ['supplierCode'], ['supplierCode']) }} as suppliercode,
    {{ json_extract_scalar('_airbyte_data', ['supplierName'], ['supplierName']) }} as suppliername,
    {{ json_extract_scalar('_airbyte_data', ['accountNumber'], ['accountNumber']) }} as accountnumber,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('halcapitaldb', '_airbyte_raw_suppliers') }} as table_alias
-- suppliers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

