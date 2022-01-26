{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('halcapitaldb', '_airbyte_raw_transactions') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['userId'], ['userId']) }} as userid,
    {{ json_extract_scalar('_airbyte_data', ['amount'], ['amount']) }} as amount,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_scalar('_airbyte_data', ['paymentId'], ['paymentId']) }} as paymentid,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['supplierId'], ['supplierId']) }} as supplierid,
    {{ json_extract_scalar('_airbyte_data', ['loanOfferId'], ['loanOfferId']) }} as loanofferid,
    {{ json_extract_scalar('_airbyte_data', ['phoneNumber'], ['phoneNumber']) }} as phonenumber,
    {{ json_extract_scalar('_airbyte_data', ['requestData'], ['requestData']) }} as requestdata,
    {{ json_extract_scalar('_airbyte_data', ['acceptedDate'], ['acceptedDate']) }} as accepteddate,
    {{ json_extract_scalar('_airbyte_data', ['rejectedDate'], ['rejectedDate']) }} as rejecteddate,
    {{ json_extract_scalar('_airbyte_data', ['ncbaReference'], ['ncbaReference']) }} as ncbareference,
    {{ json_extract_scalar('_airbyte_data', ['loanContractId'], ['loanContractId']) }} as loancontractid,
    {{ json_extract_scalar('_airbyte_data', ['transactionData'], ['transactionData']) }} as transactiondata,
    {{ json_extract_scalar('_airbyte_data', ['transactionType'], ['transactionType']) }} as transactiontype,
    {{ json_extract_scalar('_airbyte_data', ['dukareePaymentId'], ['dukareePaymentId']) }} as dukareepaymentid,
    {{ json_extract_scalar('_airbyte_data', ['contractDocumentUrl'], ['contractDocumentUrl']) }} as contractdocumenturl,
    {{ json_extract_scalar('_airbyte_data', ['paymentTransactionId'], ['paymentTransactionId']) }} as paymenttransactionid,
    {{ json_extract_scalar('_airbyte_data', ['callbackStatusResponse'], ['callbackStatusResponse']) }} as callbackstatusresponse,
    {{ json_extract_scalar('_airbyte_data', ['amount_aibyte_transform'], ['amount_aibyte_transform']) }} as amount_aibyte_transform,
    {{ json_extract_scalar('_airbyte_data', ['responseData_aibyte_transform'], ['responseData_aibyte_transform']) }} as responsedata_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('halcapitaldb', '_airbyte_raw_transactions') }} as table_alias
-- transactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

