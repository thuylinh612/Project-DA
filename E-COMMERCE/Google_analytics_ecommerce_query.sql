-- BIG PROJECT FOR SQL: EXPLORE DATA ON GA SESSION. CONDUCT AD HOC ANALYSIS.


-- QUERY 01: CALCULATE TOTAL VISIT, PAGEVIEW, TRANSACTION AND REVENUE FOR JAN, FEB AND MARCH 2017 ORDER BY MONTH


SELECT format_date("%Y%m",parse_date('%Y%m%d',date) ) as month,

         sum(totals.visits) as visits
        ,sum(totals.pageviews) as pageviews
        ,sum(totals.transactions) as transactions 
        ,sum(totals.totalTransactionRevenue /(power(10,6))  as revenue 
       
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix between '0101' and '0331'
GROUP BY month 
ORDER BY month 


-- QUERY 02: BOUNCE RATE PER TRAFFIC SOURCE IN JULY 2017

SELECT trafficSource.source,

      count(totals.visits) as total_visits,
      sum(totals.bounces) as total_bounces, 
      round(sum(totals.bounces)/count(totals.visits) * 100.0,8) as bounce_rate

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
GROUP BY trafficSource.source
ORDER BY 2 desc


-- QUERY 3: REVENUE BY TRAFFIC SOURCE BY WEEK, BY MONTH IN JUNE 2017

SELECT 'Month' AS Time_type,
        FORMAT_date("%Y%m", parse_date('%Y%m%d',date)) as time, 
        trafficSource.source as source, 
        sum(totals.totalTransactionRevenue)/(1000000) 

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
WHERE totals.totalTransactionRevenue is not null
GROUP BY trafficSource.source, FORMAT_date("%Y%m", parse_date('%Y%m%d',date))

UNION ALL

SELECT 'Week' as time_type ,
        FORMAT_date("%Y%W", parse_date('%Y%m%d',date)) as time, 
        trafficSource.source as source, 
        sum(totals.totalTransactionRevenue)/(1000000) as revenue

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
WHERE totals.totalTransactionRevenue is not null
GROUP BY trafficSource.source, FORMAT_date("%Y%W", parse_date('%Y%m%d',date))


--QUERY 04: AVERAGE NUMBER OF PRODUCT PAGEVIEWS BY PURCHASER TYPE (PURCHASERS VS NON-PURCHASERS) IN JUNE, JULY 2017. NOTE: TOTALS.TRANSACTIONS >=1 FOR PURCHASER AND TOTALS.TRANSACTIONS IS NULL FOR NON-PURCHASER

SELECT A.MONTH, AVG_PAGEVIEWS_PURCHASE,AVG_PAGEVIEWS_NON_PURCHASE 
FROM

  (SELECT format_date("%Y%m",parse_date('%Y%m%d',date) ) AS MONTH,
          sum(totals.pageviews)/count(distinct fullvisitorID) AS AVG_PAGEVIEWS_NON_PURCHASE 
  FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
  WHERE  (_TABLE_SUFFIX BETWEEN '0601' AND '0731') AND TOTALS.TRANSACTIONS IS  NULL 
  GROUP BY MONTH) A 

FULL JOIN

  (SELECT format_date("%Y%m",parse_date('%Y%m%d',date) ) AS MONTH,
          sum(totals.pageviews)/count(distinct fullvisitorID) AS AVG_PAGEVIEWS_PURCHASE 
  FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
  WHERE  (_TABLE_SUFFIX BETWEEN '0601' AND '0731') AND TOTALS.TRANSACTIONS IS NOT NULL 
  GROUP BY format_date("%Y%m",parse_date('%Y%m%d',date) )) B 
  
ON A.MONTH=B.MONTH



-- QUERY 05: AVERAGE NUMBER OF TRANSACTIONS PER USER THAT MADE A PURCHASE IN JULY 2017


SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
       ROUND(SUM(TOTALS.TRANSACTIONS)/count(distinct fullvisitorID), 4)

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`

WHERE TOTALS.TRANSACTIONS IS NOT NULL




-- QUERY 06: AVERAGE AMOUNT OF MONEY SPENT PER SESSION IN JULY 2017


SELECT '7' as month, format("%'f",round(sum(totals.totalTransactionRevenue)/count(visitId)/power(10,6),2)) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE TOTALS.TRANSACTIONS IS NOT NULL



-- QUERY 07: OTHER PRODUCTS PURCHASED BY CUSTOMERS WHO PURCHASED PRODUCT "YOUTUBE MEN'S VINTAGE HENLEY" IN JULY 2017. 
-- OUTPUT SHOULD SHOW PRODUCT NAME AND THE QUANTITY WAS ORDERED.


SELECT product.v2ProductName,
       sum(product.productQuantity) as quantity 

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
unnest(hits) as hits, 
unnest(product) as product

WHERE fullvisitorId in 

      (SELECT fullvisitorId 
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,unnest(hits) as hits,unnest(product)as product
      where  product.v2ProductName = "YouTube Men's Vintage Henley"  
          and product.productquantity is not null 
          and product.productRevenue is not null) 

AND product.productRevenue is not null
AND product.productQuantity is not null  
GROUP BY  product.v2ProductName
ORDER BY 2



--QUERY 08: CALCULATE COHORT MAP FROM PAGEVIEW TO ADDTOCART TO PURCHASE IN LAST 3 MONTH. FOR EXAMPLE, 100% PAGEVIEW THEN 40% ADD_TO_CART AND 10% PURCHASE.

--Solution 1: --use CTE to count total product views, add_to_cart and purchase

with
product_view as(
SELECT
--make sure that date is formatted in the right way
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month
join purchase p on pv.month = p.month
order by pv.month


--Solution 2: Use case when

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    sum(CASE WHEN eCommerceAction.action_type = '2' THEN 1 END) as num_product_view,
    sum(CASE WHEN eCommerceAction.action_type = '3' THEN 1 END) as num_add_to_cart,
    sum(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN 1 END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data


                                                          ---end---







