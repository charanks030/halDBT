{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('userdebittransactions_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'amount',
        'status',
        'userid',
        'usageid',
        'createdat',
        'updatedat',
        'supplierid',
        'transactionid',
        'usermastercontractid',
    ]) }} as _airbyte_userdebittransactions_hashid,
    tmp.*
from {{ ref('userdebittransactions_ab2') }} tmp
-- userdebittransactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

