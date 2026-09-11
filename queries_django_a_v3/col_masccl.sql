SELECT
    m.id,
    m.ccl_mas,
    m.ue_mas_gc_construccion,
    m.ue_mas_gc_servidumbretransito,
    m.ue_mas_gc_terreno,
    m.ue_mas_gc_unidadconstruccion
FROM {schema}.col_masccl m
LEFT JOIN {schema}.gc_lindero l ON m.ccl_mas = l.id
LEFT JOIN {schema}.gc_construccion c ON m.ue_mas_gc_construccion = c.id
LEFT JOIN {schema}.gc_servidumbretransito st ON m.ue_mas_gc_servidumbretransito = st.id
LEFT JOIN {schema}.gc_terreno t ON m.ue_mas_gc_terreno = t.id
LEFT JOIN {schema}.gc_unidadconstruccion uc ON m.ue_mas_gc_unidadconstruccion = uc.id;
