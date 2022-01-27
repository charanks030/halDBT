{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('offers_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(fees as {{ dbt_utils.type_string() }}) as fees,
    cast(amount as {{ dbt_utils.type_float() }}) as amount,
    ruleid,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(tenure as {{ dbt_utils.type_float() }}) as tenure,
    cast(enddate as {{ dbt_utils.type_string() }} ) as enddate,
    cast(partner as {{ dbt_utils.type_string() }}) as partner,
    cast({{ adapter.quote('comments') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('comments') }},
    cast(createdat as {{ dbt_utils.type_string() }}) as createdat,
    cast(createdby as {{ dbt_utils.type_string() }}) as createdby,
    cast(darkcolor as {{ dbt_utils.type_string() }}) as darkcolor,
    cast(offername as {{ dbt_utils.type_string() }}) as offername,
    cast(offertype as {{ dbt_utils.type_string() }}) as offertype,
    cast(startdate as {{ dbt_utils.type_string() }}) as startdate,
    cast(updatedat as {{ dbt_utils.type_string() }}) as updatedat,
    cast(lightcolor as {{ dbt_utils.type_string() }}) as lightcolor,
    cast(profitrate as {{ dbt_utils.type_string() }}) as profitrate,
    cast(creditrange as {{ dbt_utils.type_float() }}) as creditrange,
    cast(insurancefee as {{ dbt_utils.type_string() }}) as insurancefee,
    cast(notification as {{ dbt_utils.type_string() }}) as notification,
    cast(processingfee as {{ dbt_utils.type_string() }}) as processingfee,
    {{ cast_to_boolean('isprofitrateshow') }} as isprofitrateshow,
    {{ cast_to_boolean('isinsurancefeeshow') }} as isinsurancefeeshow,
    latepaymentfeeinfo,
    cast(commoditytradingfee as {{ dbt_utils.type_string() }}) as commoditytradingfee,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('offers_ab1') }}
-- offers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

