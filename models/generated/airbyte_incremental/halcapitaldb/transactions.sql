{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "halcapitaldb",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('transactions_ab3') }}
select
    _id,
    status,
    userid,
    amount,
    createdat,
    paymentid,
    updatedat,
    supplierid,
    loanofferid,
    phonenumber,
    requestdata,
    accepteddate,
    rejecteddate,
    ncbareference,
    loancontractid,
    transactiondata,
    transactiontype,
    dukareepaymentid,
    contractdocumenturl,
    paymenttransactionid,
    callbackstatusresponse,
    amount_aibyte_transform,
    responsedata_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_transactions_hashid
from {{ ref('transactions_ab3') }}
-- transactions from {{ source('halcapitaldb', '_airbyte_raw_transactions') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

