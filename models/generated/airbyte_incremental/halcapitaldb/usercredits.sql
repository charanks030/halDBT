{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "halcapitaldb",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('usercredits_ab3') }}
select
    _id,
    status,
    userid,
    lastdate,
    createdat,
    updatedat,
    usedamount,
    financeamount,
    transactionid,
    availablebalance,
    creditlimitproductid,
    outstandingAmount,
    outstandingamount_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_usercredits_hashid
from {{ ref('usercredits_ab3') }}
-- usercredits from {{ source('halcapitaldb', '_airbyte_raw_usercredits') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

