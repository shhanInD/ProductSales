from google.oauth2 import service_account
import json
import pandas_gbq

json_path_list = [
    "/home/ubuntu/automation/DemandForecastOrdersheets/credfile/dbwisely-v2-01bfe15ef302.json",
    "/Users/waijeulli/Downloads/dbwisely-v2-01bfe15ef302.json"
]

for path in json_path_list:
    try:
        with open(path) as f:
            json_data = json.load(f)
        json_path = path
    except:
        json_path = ""
        continue

credentials = service_account.Credentials.from_service_account_file(
    json_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"
            ]
)

query1 = f"""
select distinct productcode, optionitemname, concat(brand_contained,"_", SKUname_1) as SKU, brand_no, item_no_1, item_no_2, option_no
from `dbwisely-v2.dsCafe24.tbProducts`
where productcode is not null and SKUname_1 is not null and optionitemname not like "%칫솔%개입%"
order by brand_no, item_no_1, item_no_2, option_no
"""
itemlist = list(pandas_gbq.read_gbq(query1, project_id="dbwisely-v2", credentials=credentials, progress_bar_type= None)["SKU"].values)

query_string = ""
for item in itemlist:
    query_string += f"sum( if (SKU = '{item}', qty, 0)) as {item},"

query2 = f"""
with nthorder as (
  select order_id, 
    CASE 
        WHEN rn = 1 THEN "N1"
        WHEN rn = 2 THEN "N2"
        WHEN rn = 3 THEN "N3"
        WHEN rn >= 4 THEN "N4 이상"
    END AS nthPurchase, 
    CASE
        WHEN rn = 1 THEN "첫구매"
        ELSE "재구매"
    END AS refirstPurchase
  from (
    select distinct order_id, dense_rank() over(partition by member_id order by payment_date) rn
    from `dbwisely-v2.dtCafe24.tbOrders`
    where isValid = 1 and orderType in ("1","2", "3")
  )
),

productsold as (
  select replace(productcode, " ", "") as productcode, itemid, itemname, optionitemname, sum(quantity) as qty, 
    date(payment_date) as days, 
    date(date_trunc(payment_date, month)) as month, 
    extract(isoweek from payment_date) as weeknum,
    if(isOnetime = 1, "1회", "구독") as subsc, nthPurchase, refirstPurchase
  from `dbwisely-v2.dtCafe24.tbOrders`, unnest(item)
  left join (
    select order_id, nthPurchase, refirstPurchase
    from nthorder
  )
  using(order_id)
  where payment_date > "2022-01-01"
  and productcode is not null and length(productcode) > 0
  and isValid = 1 and orderType in ("1","2", "3")
  group by productcode, itemid, itemname, optionitemname, days, subsc, month, weeknum, nthPurchase, refirstPurchase
),

res as (
  select days,  month, weeknum, subsc, refirstPurchase,
    {query_string}
  from (
    select 
      days, month, weeknum, subsc, refirstPurchase,
      if(productcode_after is null, productcode, productcode_after) as productcode, itemid, itemname,
      if(productcode_after is null, qty, qty*multiply_after) as qty,
    from productsold
    left join (
      select cast(productcode_before as string) as productcode, cast(productcode_after as string) as productcode_after,	optionitemname_after, multiply_after
      from `dbwisely-v2.dsCafe24.tbProducts_Decomposition`
      where productcode_before not in (8809642574663,8809642574670) # 곰팡이제거제와 욕실세정제는 decomposition상품에서 제거
    )
    using(productcode)
  )
  left join (
    select cast(productcode as string) as productcode, concat(brand_contained,"_", SKUname_1) as SKU, brand_no, item_no_1, item_no_2
    from `dbwisely-v2.dsCafe24.tbProducts`
    where SKUname_1 is not null
  )
  using(productcode)
  left join (
    select distinct cast(productcode as string) as productcode, max(optionitemname) as optionitemname
    from `dbwisely-v2.dsCafe24.tbProducts`
    group by productcode
  )
  using(productcode)
  group by days, month, weeknum, subsc, refirstPurchase
)

select *
from res
order by days desc, subsc, refirstPurchase desc
"""

df1 = pandas_gbq.read_gbq(query2, project_id="dbwisely-v2", credentials=credentials, progress_bar_type= None)

query3 = f"""
with nthorder as (
  select order_id, 
    CASE 
        WHEN rn = 1 THEN "N1"
        WHEN rn = 2 THEN "N2"
        WHEN rn = 3 THEN "N3"
        WHEN rn >= 4 THEN "N4 이상"
    END AS nthPurchase, 
    CASE
        WHEN rn = 1 THEN "첫구매"
        ELSE "재구매"
    END AS refirstPurchase
  from (
    select distinct order_id, dense_rank() over(partition by member_id order by payment_date) rn
    from `dbwisely-v2.dtCafe24.tbOrders`
    where isValid = 1 and orderType in ("1","2", "3")
  )
),

multistore as (
  select distinct member_id, if(date(first_value(payment_date) over (partition by member_id order by payment_date)) < "2022-03-03", "통합스토어 이전 고객", "통합스토어 이후 고객") as renewal
  from `dbwisely-v2.dtCafe24.tbOrders`
  where isValid = 1 and orderType in ("1","2")
  order by member_id
),

productsold as (
  select replace(productcode, " ", "") as productcode, itemid, itemname, optionitemname, sum(quantity) as qty, 
    date(payment_date) as days, 
    date(date_trunc(payment_date, month)) as month, 
    extract(isoweek from payment_date) as weeknum,
    if(isOnetime = 1, "1회", "구독") as subsc, nthPurchase, refirstPurchase, renewal
  from `dbwisely-v2.dtCafe24.tbOrders`, unnest(item)
  left join (
    select order_id, nthPurchase, refirstPurchase
    from nthorder
  )
  using(order_id)
  left join (
    select member_id, renewal
    from multistore
  )
  using(member_id)
  where payment_date > "2022-01-01"
  and productcode is not null and length(productcode) > 0
  and isValid = 1 and orderType in ("1","2", "3")
  group by productcode, itemid, itemname, optionitemname, days, subsc, month, weeknum, nthPurchase, refirstPurchase, renewal
),

res as (
  select days,  month, weeknum, subsc, nthPurchase, renewal,
    {query_string}
  from (
    select 
      days, month, weeknum, subsc, nthPurchase, renewal,
      if(productcode_after is null, productcode, productcode_after) as productcode, itemid, itemname,
      if(productcode_after is null, qty, qty*multiply_after) as qty,
    from productsold
    left join (
      select cast(productcode_before as string) as productcode, cast(productcode_after as string) as productcode_after,	optionitemname_after, multiply_after
      from `dbwisely-v2.dsCafe24.tbProducts_Decomposition`
      where productcode_before not in (8809642574663,8809642574670) # 곰팡이제거제와 욕실세정제는 decomposition상품에서 제거
    )
    using(productcode)
  )
  left join (
    select cast(productcode as string) as productcode, concat(brand_contained,"_", SKUname_1) as SKU, brand_no, item_no_1, item_no_2
    from `dbwisely-v2.dsCafe24.tbProducts`
    where SKUname_1 is not null
  )
  using(productcode)
  left join (
    select distinct cast(productcode as string) as productcode, max(optionitemname) as optionitemname
    from `dbwisely-v2.dsCafe24.tbProducts`
    group by productcode
  )
  using(productcode)
  group by days, month, weeknum, subsc, nthPurchase, renewal
)

select *
from res
order by days desc, renewal, subsc, nthPurchase desc
"""

df2 = pandas_gbq.read_gbq(query3, project_id="dbwisely-v2", credentials=credentials, progress_bar_type= None)

credentials = service_account.Credentials.from_service_account_file(json_path)
project_id = "dbwisely-v2"
#새로 테이블 만들것이라면 굳이 빅쿼리에서 미리 스킴 짤필요 없이 여기서 테이블 이름 지정하면 됩니다.
destination_table1 = f'dmCafe24.tbSKU_Performance'
destination_table2 = f'dmCafe24.tbSKU_Performance_SCM'

# print("데이터 빅쿼리로 옮길 준비")
pandas_gbq.to_gbq(df1, destination_table1,project_id,if_exists="replace",credentials=credentials, progress_bar=False)
pandas_gbq.to_gbq(df2, destination_table2,project_id,if_exists="replace",credentials=credentials, progress_bar=False)
# print("빅쿼리로 이관 완료")
