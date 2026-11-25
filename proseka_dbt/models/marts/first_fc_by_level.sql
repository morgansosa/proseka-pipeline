select
    ch.level,
    min(cl.clear_date) as first_fc_date,
    count(*) as fc_count
from {{ ref('stg_clears') }} as cl
join {{ ref('stg_charts') }} as ch
    on  cl.music_id = ch.music_id
    and cl.server = ch.server
    and cl.difficulty = ch.difficulty
where cl.clear_type = 'FC'
group by ch.level
order by ch.level
