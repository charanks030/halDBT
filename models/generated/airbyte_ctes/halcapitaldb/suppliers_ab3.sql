{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_halcapitaldb",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('suppliers_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'bankcode',
        'swiftcode',
        'branchcode',
        'otherdetails',
        'suppliercode',
        'suppliername',
        'accountnumber',
    ]) }} as _airbyte_suppliers_hashid,
    tmp.*
from {{ ref('suppliers_ab2') }} tmp
-- suppliers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

