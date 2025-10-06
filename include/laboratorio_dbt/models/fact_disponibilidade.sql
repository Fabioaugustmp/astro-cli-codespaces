{{
  config(
    materialized='table',
    schema='core'
  )
}}

-- Modelo de fatos centralizado: fact_disponibilidade
-- Grão: por usina, localidade e instante (timestamp)

with src as (
  select
    id_subsistema,
    nom_subsistema,
    id_estado,
    nom_estado,
    nom_usina,
    nom_tipocombustivel,
    ceg,
    din_instante::timestamp as instante,
    val_potenciainstalada as pot_instalada_mw,
    val_dispoperacional as disp_operacional_mw,
    val_dispsincronizada as disp_sincronizada_mw
  from {{ source('staging','stg_usina_disp') }}
),

usina_dim as (
  select id_dim_usina, nom_usina, nom_tipocombustivel, ceg
  from {{ ref('dim_usina') }}
),

localidade_dim as (
  select id_dim_localidade, nom_subsistema, nom_estado
  from {{ ref('dim_localidade') }}
),

tempo_dim as (
  select id_dim_tempo, instante as instante_dim, ano, mes, dia
  from {{ ref('dim_tempo') }}
),

joined as (
  select
    s.*,
    u.id_dim_usina,
    l.id_dim_localidade,
    t.id_dim_tempo,
    date_trunc('day', s.instante) as instante_dia
  from src s
  left join usina_dim u
    on s.nom_usina = u.nom_usina
    and s.nom_tipocombustivel = u.nom_tipocombustivel
    and s.ceg = u.ceg
  left join localidade_dim l
    on s.nom_subsistema = l.nom_subsistema
    and s.nom_estado = l.nom_estado
  left join tempo_dim t
    on s.instante = t.instante_dim
)

select
  id_dim_usina,
  id_dim_localidade,
  id_dim_tempo,
  instante,
  instante_dia,
  nom_usina,
  nom_subsistema,
  nom_estado,
  nom_tipocombustivel,
  ceg,
  pot_instalada_mw,
  disp_operacional_mw,
  disp_sincronizada_mw
from joined

order by instante, id_dim_usina, id_dim_localidade
