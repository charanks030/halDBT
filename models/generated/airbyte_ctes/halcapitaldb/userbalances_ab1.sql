{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('halcapitaldb', '_airbyte_raw_userbalances') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['userId'], ['userId']) }} as userid,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['usedAmount'], ['usedAmount']) }} as usedamount,
    {{ json_extract_scalar('_airbyte_data', ['financeAmount'], ['financeAmount']) }} as financeamount,
    {{ json_extract_scalar('_airbyte_data', ['availableBalance'], ['availableBalance']) }} as availablebalance,
    {{ json_extract_scalar('_airbyte_data', ['outstandingAmount'], ['outstandingAmount']) }} as outstandingAmount,
    {{ json_extract_scalar('_airbyte_data', ['outstandingAmount_aibyte_transform'], ['outstandingAmount_aibyte_transform']) }} as outstandingamount_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('halcapitaldb', '_airbyte_raw_userbalances') }} as table_alias
-- userbalances
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

