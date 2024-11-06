# SQL-PANDAS

from kaggle using api to pull dataset to local device and then loading it in pandas to the sql server and then solving queries 


      




  select * from df_orders

--find top 10 highest revenue generating products
select top 10 product_id, sum(sale_price) rev
from df_orders
group by product_id
order by  rev desc

--find top 5 highest selling products in each region
with cte as(
select  region, product_id, sum(sale_price) rev
from df_orders
group by region,product_id
) 
select * from (
select *, ROW_NUMBER() over(partition by region order by rev desc) as rank
from cte ) a
where rank<=5

--find month over month growth comparison for 2022 and 2023 sales eg: jan 2022 vs jan 2023
with cte1 as (
select distinct year(order_date) order_year,month(order_date) order_month,sum(sale_price) sales_price
from df_orders
group by year(order_date),month(order_date)
)
select order_month,
sum(case when order_year=2022 then sales_price else 0 end) as sales_2022,
sum(case when order_year=2023 then sales_price else 0 end) as sales_2023
from cte1
group by order_month
order by order_month

--for each category which month had highest sales
with cte2 as (select category,format(order_date,'yyyy-MM') order_year_month,sum(sale_price) sale
from df_orders
group by category,format(order_date,'yyyy-MM')),
cte_rank as(
select category, order_year_month,sale,
rank() over(partition by category order by sale desc) sale_rank
from cte2
)
select category,order_year_month,sale
from cte_rank
where sale_rank=1
order by category

--which sub category had highest growth by profit in 2023 compare to 2022
with year_trend as (select sub_category,year(order_date) order_year,sum(sale_price) sale
from df_orders
group by sub_category,year(order_date)),
year_sales as (select sub_category,
sum(case when order_year=2022 then sale else 0 end) as sale_2022,
sum(case when order_year=2023 then sale else 0 end) as sale_2023
from year_trend
group by sub_category
)
select top 1 *,
(sale_2023 - sale_2022) growth
from year_sales
order by (sale_2023 - sale_2022) desc

