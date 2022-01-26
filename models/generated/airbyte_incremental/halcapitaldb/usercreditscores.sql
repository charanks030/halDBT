{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "halcapitaldb",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('usercreditscores_ab3') }}
select
    _id,
    {{ adapter.quote('data') }},
    status,
    userid,
    createdat,
    createdby,
    updatedat,
    modifiedat,
    modifiedby,
    score_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_usercreditscores_hashid
from {{ ref('usercreditscores_ab3') }}
-- usercreditscores from {{ source('halcapitaldb', '_airbyte_raw_usercreditscores') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

