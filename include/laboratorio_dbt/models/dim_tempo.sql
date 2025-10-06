{{
	config(
		materialized='table',
		schema='core'
	)
}}

-- Dimensão de tempo construída a partir da coluna din_instante unida de todos os meses
with distinct_instantes as (
	select distinct din_instante
	from {{ source('staging','stg_usina_disp') }}
	where din_instante is not null
)

select
	{{ dbt_utils.generate_surrogate_key(['din_instante']) }} as id_dim_tempo,
	din_instante as instante,
	extract(year from instante)::int as ano,
	extract(month from instante)::int as mes,
	extract(day from instante)::int as dia	
from distinct_instantes
order by din_instante
