{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('offers_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'fees',
        'amount',
        array_to_string('ruleid'),
        'status',
        'tenure',
        'enddate',
        'partner',
        adapter.quote('comments'),
        'createdat',
        'createdby',
        'darkcolor',
        'offername',
        'offertype',
        'startdate',
        'updatedat',
        'lightcolor',
        'profitrate',
        'creditrange',
        'insurancefee',
        'notification',
        'processingfee',
        boolean_to_string('isprofitrateshow'),
        boolean_to_string('isinsurancefeeshow'),
        array_to_string('latepaymentfeeinfo'),
        'commoditytradingfee',
    ]) }} as _airbyte_offers_hashid,
    tmp.*
from {{ ref('offers_ab2') }} tmp
-- offers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

