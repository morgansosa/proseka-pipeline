select
	music_id,
	server, 
	difficulty, 
	clear_type,
	cast(date as date) as clear_date, 
	date_is_upper_bound = 1 as is_upper_bound,
	note as clear_note
from {{ source('proseka', 'clears') }}	
