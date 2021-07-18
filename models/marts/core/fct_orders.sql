with orders as
(
    select order_id,
            customer_id 
    from {{ ref('stg_orders') }} 
),
payments as 
(
    select  order_id,
            payment_id,
            amount
         from {{ ref('stg_payments') }}
),
final as
(
    select 
        orders.order_id,
        orders.customer_id,
        payments.amount

    from orders 
    inner join payments using (order_id)
)
select * from final