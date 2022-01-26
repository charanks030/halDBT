{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('phonelists_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'sign',
        'createdat',
        'updatedat',
        'phonenumber',
    ]) }} as _airbyte_phonelists_hashid,
    tmp.*
from {{ ref('phonelists_ab2') }} tmp
-- phonelists
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

