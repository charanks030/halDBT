{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('usercreditscores_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        adapter.quote('data'),
        'status',
        'userid',
        'createdat',
        'createdby',
        'updatedat',
        'modifiedat',
        'modifiedby',
        'score_aibyte_transform',
    ]) }} as _airbyte_usercreditscores_hashid,
    tmp.*
from {{ ref('usercreditscores_ab2') }} tmp
-- usercreditscores
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

