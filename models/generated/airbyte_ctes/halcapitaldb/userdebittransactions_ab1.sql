{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('halcapitaldb', '_airbyte_raw_userdebittransactions') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['amount'], ['amount']) }} as amount,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['userId'], ['userId']) }} as userid,
    {{ json_extract_scalar('_airbyte_data', ['usageId'], ['usageId']) }} as usageid,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['supplierId'], ['supplierId']) }} as supplierid,
    {{ json_extract_scalar('_airbyte_data', ['transactionId'], ['transactionId']) }} as transactionid,
    {{ json_extract_scalar('_airbyte_data', ['userMasterContractId'], ['userMasterContractId']) }} as usermastercontractid,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('halcapitaldb', '_airbyte_raw_userdebittransactions') }} as table_alias
-- userdebittransactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

