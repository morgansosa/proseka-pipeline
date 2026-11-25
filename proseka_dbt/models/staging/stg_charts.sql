select
    c.music_id,
    c.server,
    s.title,
    s.pronunciation,
    c.difficulty,
    c.level,
    c.notes as note_count,
    s.published_at
from {{ source('proseka', 'charts') }} c
join {{ source('proseka', 'songs') }} s
  on s.music_id = c.music_id and s.server = c.server
