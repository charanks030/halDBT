{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('halcapitaldb', '_airbyte_raw_usercredittransactions') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['amount'], ['amount']) }} as amount,
    {{ json_extract_scalar('_airbyte_data', ['paidAt'], ['paidAt']) }} as paidat,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['userId'], ['userId']) }} as userid,
    {{ json_extract_scalar('_airbyte_data', ['product'], ['product']) }} as product,
    {{ json_extract_scalar('_airbyte_data', ['lastDate'], ['lastDate']) }} as lastdate,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['paidAmount'], ['paidAmount']) }} as paidAmount,
    {{ json_extract_scalar('_airbyte_data', ['contractId'], ['contractId']) }} as contractid,
    {{ json_extract_scalar('_airbyte_data', ['transactionId'], ['transactionId']) }} as transactionid,
    {{ json_extract_scalar('_airbyte_data', ['feesProcessing'], ['feesProcessing']) }} as feesProcessing,
    {{ json_extract_scalar('_airbyte_data', ['feesLatePayment'], ['feesLatePayment']) }} as feesLatePayment,
    {{ json_extract_scalar('_airbyte_data', ['feesFinanceCharges'], ['feesFinanceCharges']) }} as feesFinanceCharges,
    {{ json_extract_scalar('_airbyte_data', ['commodityTradingFee'], ['commodityTradingFee']) }} as commodityTradingFee,
    {{ json_extract_scalar('_airbyte_data', ['contractDocumentUrl'], ['contractDocumentUrl']) }} as contractdocumenturl,
    {{ json_extract_scalar('_airbyte_data', ['commodityMurabahStatus'], ['commodityMurabahStatus']) }} as commoditymurabahstatus,
    {{ json_extract_scalar('_airbyte_data', ['commodityMurabahBatchId'], ['commodityMurabahBatchId']) }} as commoditymurabahbatchid,
    {{ json_extract_scalar('_airbyte_data', ['commodityMurabahParcelId'], ['commodityMurabahParcelId']) }} as commoditymurabahparcelid,
    {{ json_extract_scalar('_airbyte_data', ['paidAmount_aibyte_transform'], ['paidAmount_aibyte_transform']) }} as paidamount_aibyte_transform,
    {{ json_extract_scalar('_airbyte_data', ['commodityMurabahRequestedDate'], ['commodityMurabahRequestedDate']) }} as commoditymurabahrequesteddate,
    {{ json_extract_scalar('_airbyte_data', ['feesProcessing_aibyte_transform'], ['feesProcessing_aibyte_transform']) }} as feesprocessing_aibyte_transform,
    {{ json_extract_scalar('_airbyte_data', ['feesLatePayment_aibyte_transform'], ['feesLatePayment_aibyte_transform']) }} as feeslatepayment_aibyte_transform,
    {{ json_extract_scalar('_airbyte_data', ['feesFinanceCharges_aibyte_transform'], ['feesFinanceCharges_aibyte_transform']) }} as feesfinancecharges_aibyte_transform,
    {{ json_extract_scalar('_airbyte_data', ['commodityTradingFee_aibyte_transform'], ['commodityTradingFee_aibyte_transform']) }} as commoditytradingfee_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('halcapitaldb', '_airbyte_raw_usercredittransactions') }} as table_alias
-- usercredittransactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

