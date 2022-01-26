{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('transactions_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'status',
        'userid',
        'createdat',
        'paymentid',
        'updatedat',
        'supplierid',
        'loanofferid',
        'phonenumber',
        'requestdata',
        'accepteddate',
        'rejecteddate',
        'ncbareference',
        'loancontractid',
        'transactiondata',
        'transactiontype',
        'dukareepaymentid',
        'contractdocumenturl',
        'paymenttransactionid',
        'callbackstatusresponse',
        'amount_aibyte_transform',
        'responsedata_aibyte_transform',
    ]) }} as _airbyte_transactions_hashid,
    tmp.*
from {{ ref('transactions_ab2') }} tmp
-- transactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

