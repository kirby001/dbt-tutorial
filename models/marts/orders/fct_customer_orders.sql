with

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

payment as (

    select * from {{ ref('stg_stripe__payment') }}

),

customers_first_order as (

    select
        customers.*,
        min(orders.order_placed_at) as fdos
    
    from customers 
    
    left join orders on orders.customer_id = customers.customer_id 

    group by 
        customers.customer_id,
        customers.customer_first_name,
        customers.customer_last_name

),

final as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        payment.total_amount_paid,
        payment.payment_finalized_date,
        customers_first_order.customer_first_name,
        customers_first_order.customer_last_name,
        row_number() over (order by orders.order_id) as transaction_seq,
        row_number() over (partition by orders.customer_id order by orders.order_id) as customer_sales_seq,
        case
            when customers_first_order.fdos = orders.order_placed_at then 'new'
            else 'return'
        end as nvsr,
        sum(payment.total_amount_paid) over (partition by orders.customer_id order by orders.order_id) as customer_lifetime_value,
        customers_first_order.fdos

    from orders

    left join payment ON orders.order_id = payment.order_id

    left join customers_first_order on orders.customer_id = customers_first_order.customer_id
    
)

select * from final order by final.order_id
