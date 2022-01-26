{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('halcapitaldb', '_airbyte_raw_usercreditscores') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['data'], ['data']) }} as {{ adapter.quote('data') }},
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['userId'], ['userId']) }} as userid,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_scalar('_airbyte_data', ['createdBy'], ['createdBy']) }} as createdby,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['modifiedAt'], ['modifiedAt']) }} as modifiedat,
    {{ json_extract_scalar('_airbyte_data', ['modifiedBy'], ['modifiedBy']) }} as modifiedby,
    {{ json_extract_scalar('_airbyte_data', ['score'], ['score']) }} as score,
    {{ json_extract_scalar('_airbyte_data', ['score_aibyte_transform'], ['score_aibyte_transform']) }} as score_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('halcapitaldb', '_airbyte_raw_usercreditscores') }} as table_alias
-- usercreditscores
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

