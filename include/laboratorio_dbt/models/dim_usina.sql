{{
	config(
		materialized='table',
		schema='core'
	)
}}

-- Dimensão de usina construída a partir da união de todos os meses ingeridos
-- Normaliza nomes de colunas e remove duplicatas mantendo só atributos únicos
with distinct_usinas as (
	select distinct
		nom_usina,
		nom_tipocombustivel,
		ceg
	from {{ source('staging','stg_usina_disp') }}
	where nom_usina is not null
)

select
	{{ dbt_utils.generate_surrogate_key(['nom_usina','nom_tipocombustivel','ceg']) }} as id_dim_usina,
	nom_usina,
	nom_tipocombustivel,
	ceg
from distinct_usinas

order by nom_usina
