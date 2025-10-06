{{
	config(
		materialized='table',
		schema='core'
	)
}}

-- Dimensão de localidade: união de meses e atributos únicos
with distinct_localidades as (
	select distinct
		nom_subsistema,
		nom_estado
	from {{ source('staging','stg_usina_disp') }}
	where nom_subsistema is not null
)

select
	{{ dbt_utils.generate_surrogate_key(['nom_subsistema','nom_estado']) }} as id_dim_localidade,
	nom_subsistema,
	nom_estado
from distinct_localidades

order by nom_subsistema
