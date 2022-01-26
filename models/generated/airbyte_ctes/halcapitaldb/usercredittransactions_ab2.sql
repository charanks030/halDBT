{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('usercredittransactions_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(amount as {{ dbt_utils.type_float() }}) as amount,
    cast(paidat as {{ dbt_utils.type_timestamp() }}) as paidat,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(userid as {{ dbt_utils.type_string() }}) as userid,
    cast(product as {{ dbt_utils.type_string() }}) as product,
    cast(lastdate as {{ dbt_utils.type_timestamp() }}) as lastdate,
    cast(createdat as {{ dbt_utils.type_timestamp() }}) as createdat,
    cast(updatedat as {{ dbt_utils.type_timestamp() }}) as updatedat,
    cast(paidAmount as {{ dbt_utils.type_float() }}) as paidAmount,
    cast(contractid as {{ dbt_utils.type_string() }}) as contractid,
    cast(transactionid as {{ dbt_utils.type_string() }}) as transactionid,
    cast(feesProcessing as {{ dbt_utils.type_float() }}) as feesProcessing,
    cast(feesLatePayment as {{ dbt_utils.type_string() }}) as feesLatePayment,
    cast(feesFinanceCharges as {{ dbt_utils.type_float() }}) as feesFinanceCharges,
    cast(commodityTradingFee as {{ dbt_utils.type_float() }}) as commodityTradingFee,
    cast(contractdocumenturl as {{ dbt_utils.type_string() }}) as contractdocumenturl,
    cast(commoditymurabahstatus as {{ dbt_utils.type_string() }}) as commoditymurabahstatus,
    cast(commoditymurabahbatchid as {{ dbt_utils.type_string() }}) as commoditymurabahbatchid,
    cast(commoditymurabahparcelid as {{ dbt_utils.type_string() }}) as commoditymurabahparcelid,
    cast(paidamount_aibyte_transform as {{ dbt_utils.type_string() }}) as paidamount_aibyte_transform,
    cast(commoditymurabahrequesteddate as {{ dbt_utils.type_string() }}) as commoditymurabahrequesteddate,
    cast(feesprocessing_aibyte_transform as {{ dbt_utils.type_string() }}) as feesprocessing_aibyte_transform,
    cast(feeslatepayment_aibyte_transform as {{ dbt_utils.type_string() }}) as feeslatepayment_aibyte_transform,
    cast(feesfinancecharges_aibyte_transform as {{ dbt_utils.type_string() }}) as feesfinancecharges_aibyte_transform,
    cast(commoditytradingfee_aibyte_transform as {{ dbt_utils.type_string() }}) as commoditytradingfee_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('usercredittransactions_ab1') }}
-- usercredittransactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

