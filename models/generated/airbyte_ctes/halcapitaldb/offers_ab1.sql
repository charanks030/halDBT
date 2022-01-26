{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('halcapitaldb', '_airbyte_raw_offers') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['fees'], ['fees']) }} as fees,
    {{ json_extract_scalar('_airbyte_data', ['amount'], ['amount']) }} as amount,
    {{ json_extract_array('_airbyte_data', ['ruleId'], ['ruleId']) }} as ruleid,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['tenure'], ['tenure']) }} as tenure,
    {{ json_extract_scalar('_airbyte_data', ['endDate'], ['endDate']) }} as enddate,
    {{ json_extract_scalar('_airbyte_data', ['partner'], ['partner']) }} as partner,
    {{ json_extract_scalar('_airbyte_data', ['comments'], ['comments']) }} as {{ adapter.quote('comments') }},
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_scalar('_airbyte_data', ['createdBy'], ['createdBy']) }} as createdby,
    {{ json_extract_scalar('_airbyte_data', ['darkColor'], ['darkColor']) }} as darkcolor,
    {{ json_extract_scalar('_airbyte_data', ['offerName'], ['offerName']) }} as offername,
    {{ json_extract_scalar('_airbyte_data', ['offerType'], ['offerType']) }} as offertype,
    {{ json_extract_scalar('_airbyte_data', ['startDate'], ['startDate']) }} as startdate,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['lightColor'], ['lightColor']) }} as lightcolor,
    {{ json_extract_scalar('_airbyte_data', ['profitRate'], ['profitRate']) }} as profitrate,
    {{ json_extract_scalar('_airbyte_data', ['creditRange'], ['creditRange']) }} as creditrange,
    {{ json_extract_scalar('_airbyte_data', ['insuranceFee'], ['insuranceFee']) }} as insurancefee,
    {{ json_extract_scalar('_airbyte_data', ['notification'], ['notification']) }} as notification,
    {{ json_extract_scalar('_airbyte_data', ['processingFee'], ['processingFee']) }} as processingfee,
    {{ json_extract_scalar('_airbyte_data', ['isProfitrateShow'], ['isProfitrateShow']) }} as isprofitrateshow,
    {{ json_extract_scalar('_airbyte_data', ['isInsuranceFeeShow'], ['isInsuranceFeeShow']) }} as isinsurancefeeshow,
    {{ json_extract_array('_airbyte_data', ['latepaymentFeeInfo'], ['latepaymentFeeInfo']) }} as latepaymentfeeinfo,
    {{ json_extract_scalar('_airbyte_data', ['commodityTradingFee'], ['commodityTradingFee']) }} as commoditytradingfee,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('halcapitaldb', '_airbyte_raw_offers') }} as table_alias
-- offers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

