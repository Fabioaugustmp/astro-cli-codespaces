SELECT
  id_subsistema,
  nom_estado,
  nom_usina,
  din_instante::TIMESTAMP       AS instante,
  val_potenciainstalada         AS pot_instalada_mw,
  val_dispoperacional           AS disp_operacional_mw,
  val_dispsincronizada          AS disp_sincronizada_mw
FROM {{ source('staging','disponibilidade_usina_2025_08') }}
