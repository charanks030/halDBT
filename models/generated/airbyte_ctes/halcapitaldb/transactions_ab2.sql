{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('transactions_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(userid as {{ dbt_utils.type_string() }}) as userid,
    cast(amount as {{ dbt_utils.type_float() }}) as amount,
    cast(createdat as {{ dbt_utils.type_timestamp() }}) as createdat,
    cast(paymentid as {{ dbt_utils.type_string() }}) as paymentid,
    cast(updatedat as {{ dbt_utils.type_timestamp() }}) as updatedat,
    cast(supplierid as {{ dbt_utils.type_string() }}) as supplierid,
    cast(loanofferid as {{ dbt_utils.type_string() }}) as loanofferid,
    cast(phonenumber as {{ dbt_utils.type_string() }}) as phonenumber,
    cast(requestdata as {{ dbt_utils.type_string() }}) as requestdata,
    cast(accepteddate as {{ dbt_utils.type_timestamp() }}) as accepteddate,
    cast(rejecteddate as {{ dbt_utils.type_timestamp() }}) as rejecteddate,
    cast(ncbareference as {{ dbt_utils.type_string() }}) as ncbareference,
    cast(loancontractid as {{ dbt_utils.type_string() }}) as loancontractid,
    cast(transactiondata as {{ dbt_utils.type_string() }}) as transactiondata,
    cast(transactiontype as {{ dbt_utils.type_string() }}) as transactiontype,
    cast(dukareepaymentid as {{ dbt_utils.type_string() }}) as dukareepaymentid,
    cast(contractdocumenturl as {{ dbt_utils.type_string() }}) as contractdocumenturl,
    cast(paymenttransactionid as {{ dbt_utils.type_string() }}) as paymenttransactionid,
    cast(callbackstatusresponse as {{ dbt_utils.type_string() }}) as callbackstatusresponse,
    cast(amount_aibyte_transform as {{ dbt_utils.type_float() }}) as amount_aibyte_transform,
    cast(responsedata_aibyte_transform as {{ dbt_utils.type_string() }}) as responsedata_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('transactions_ab1') }}
-- transactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

