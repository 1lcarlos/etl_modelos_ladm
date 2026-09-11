SELECT
    m.id,
    m.ccl_menos,
    m.ue_menos_gc_construccion,
    m.ue_menos_gc_servidumbretransito,
    m.ue_menos_gc_terreno,
    m.ue_menos_gc_unidadconstruccion
FROM {schema}.col_menosccl m
LEFT JOIN {schema}.gc_lindero l ON m.ccl_menos = l.id
LEFT JOIN {schema}.gc_construccion c ON m.ue_menos_gc_construccion = c.id
LEFT JOIN {schema}.gc_servidumbretransito st ON m.ue_menos_gc_servidumbretransito = st.id
LEFT JOIN {schema}.gc_terreno t ON m.ue_menos_gc_terreno = t.id
LEFT JOIN {schema}.gc_unidadconstruccion uc ON m.ue_menos_gc_unidadconstruccion = uc.id;
