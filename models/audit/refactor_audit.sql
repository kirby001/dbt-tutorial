{% set old_query %}
  select
    order_id,
    customer_id,
    order_placed_at,
    order_status,
    total_amount_paid,
    payment_finalized_date,
    customer_first_name,
    customer_last_name,
    transaction_seq,
    customer_sales_seq,
    nvsr,
    customer_lifetime_value,
    fdos
  from analytics.dbt_dev.customer_orders
{% endset %}

{% set new_query %}
  select
    order_id,
    customer_id,
    order_placed_at,
    order_status,
    total_amount_paid,
    payment_finalized_date,
    customer_first_name,
    customer_last_name,
    transaction_seq,
    customer_sales_seq,
    nvsr,
    customer_lifetime_value,
    fdos
  from {{ ref('fct_customer_orders') }}
{% endset %}

{{ 
  audit_helper.compare_and_classify_query_results(
    old_query, 
    new_query, 
    primary_key_columns=['order_id'], 
    columns=['order_id', 'customer_lifetime_value'],
    sample_limit=100
  )
}}

-- customer_id', 'order_placed_at', 'order_status', 'total_amount_paid', 'payment_finalized_date', 'customer_first_name', 'customer_last_name', 'transaction_seq', 'customer_sales_seq', 'nvsr', 