SELECT
    m.id,
    m.ccl_mas as id_lindero,
    m.ue_mas_gc_terreno as id_terreno,
    m.ue_mas_gc_unidadconstruccion as id_unidad_construccion
FROM {schema}.col_masccl m
LEFT JOIN {schema}.gc_lindero l ON m.ccl_mas = l.id
LEFT JOIN {schema}.gc_terreno t ON m.ue_mas_gc_terreno = t.id
LEFT JOIN {schema}.gc_unidadconstruccion uc ON m.ue_mas_gc_unidadconstruccion = uc.id;
