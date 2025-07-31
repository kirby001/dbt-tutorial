with

source as (

    select * from {{ source('stripe', 'payment') }}

),

transform as (

    select 
        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    
    from source
    
    where STATUS <> 'fail'
    
    group by 1

)

select * from transform
