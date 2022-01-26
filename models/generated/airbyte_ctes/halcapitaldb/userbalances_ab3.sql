{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('userbalances_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'userid',
        'createdat',
        'updatedat',
        'usedamount',
        'financeamount',
        'availablebalance',
        'outstandingamount_aibyte_transform',
    ]) }} as _airbyte_userbalances_hashid,
    tmp.*
from {{ ref('userbalances_ab2') }} tmp
-- userbalances
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

