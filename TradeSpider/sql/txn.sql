with latest_txn (datetime, commodity_id, expired_date) as (
    select datetime,
           commodity_id,
           min(expired_date)
    from futures.txn_stage
    where length(expired_date) = 6
    group by datetime, commodity_id
)

insert into futures.txn
select t.datetime,
       t.commodity_id,
       open_price,
       high_price,
       low_price,
       close_price,
       volume
from futures.txn_stage as t
         join latest_txn as l
              on t.datetime = l.datetime
                  and t.commodity_id = l.commodity_id
                  and t.expired_date = l.expired_date
on conflict do nothing ;

