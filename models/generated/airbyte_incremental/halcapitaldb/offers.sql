{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "halcapitaldb",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('offers_ab3') }}
select
    _id,
    fees,
    amount,
    ruleid,
    status,
    tenure,
    enddate,
    partner,
    {{ adapter.quote('comments') }},
    createdat,
    createdby,
    darkcolor,
    offername,
    offertype,
    startdate,
    updatedat,
    lightcolor,
    profitrate,
    creditrange,
    insurancefee,
    notification,
    processingfee,
    isprofitrateshow,
    isinsurancefeeshow,
    latepaymentfeeinfo,
    commoditytradingfee,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_offers_hashid
from {{ ref('offers_ab3') }}
-- offers from {{ source('halcapitaldb', '_airbyte_raw_offers') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

