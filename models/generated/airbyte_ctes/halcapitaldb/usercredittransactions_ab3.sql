{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('usercredittransactions_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'amount',
        'paidat',
        'status',
        'userid',
        'product',
        'lastdate',
        'createdat',
        'updatedat',
        'paidAmount',
        'contractid',
        'transactionid',
        'feesProcessing',
        'feesLatePayment',
        'feesFinanceCharges',
        'commodityTradingFee',
        'contractdocumenturl',
        'commoditymurabahstatus',
        'commoditymurabahbatchid',
        'commoditymurabahparcelid',
        'paidamount_aibyte_transform',
        'commoditymurabahrequesteddate',
        'feesprocessing_aibyte_transform',
        'feeslatepayment_aibyte_transform',
        'feesfinancecharges_aibyte_transform',
        'commoditytradingfee_aibyte_transform',
    ]) }} as _airbyte_usercredittransactions_hashid,
    tmp.*
from {{ ref('usercredittransactions_ab2') }} tmp
-- usercredittransactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

