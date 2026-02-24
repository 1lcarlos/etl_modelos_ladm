-- Consulta optimizada con lógica de prioridad para destinacion_economica
INSERT INTO {schema}.gc_predio (
    t_id,  
    t_ili_tid,  
    departamento,  
    municipio,  
    codigo_orip,  
    matricula_inmobiliaria,  
    numero_predial_nacional,  
    codigo_homologado,  
    nupre, 
    tipo_predio,  
    condicion_predio,  
    destinacion_economica,  
    area_catastral_terreno,  
    area_registral_m2, 
    vigencia_actualizacion_catastral,  
    estado,  
    categoria_suelo,  
    clase_suelo,  
    numero_predial_anterior,  
    area_construida,  
    nombre,  
    tipo,  
    comienzo_vida_util_version,  
    fin_vida_util_version,  
    espacio_de_nombres,  
    local_id
) 
SELECT  
    nextval('{schema}.t_ili2db_seq'::regclass),  
    uuid_generate_v4(),  
    departamento,  
    municipio,  
    codigo_orip,  
    matricula_inmobiliaria::numeric, 
    numero_predial_nacional,  
    nupre,  
    nupre,  
    tp.t_id as Tipo_Predio,  
    cpt.t_id as condicion_Predio,  
    det.t_id as destinacion_economica,  
    area_catastral_terreno::numeric,  
    area_registral::numeric, 
    vigencia_actualizacion_catastral::date,  
    et.t_id as estado,  
    cst.t_id as categoria_suelo,  
    clst.t_id as clase_suelo,  
    numero_predial_anterior,  
    area_construida::numeric, 
    nombre,  
    ubat.t_id as tipo,  
    comienzo_vida_util_version::timestamp,  
    fin_vida_util_version::timestamp,  
    espacio_de_nombres,  
    id 
FROM tmp_predio p 

-- JOIN con lógica de prioridad para destinacion_economica
JOIN (
    SELECT DISTINCT ON (p_destinacion_economica) 
        t_id,
        p_destinacion_economica,
        prioridad
    FROM (
        -- Prioridad 1: Coincidencia exacta
        SELECT 
            det1.t_id,
            p1.destinacion_economica as p_destinacion_economica,
            1 as prioridad
        FROM (SELECT DISTINCT destinacion_economica FROM tmp_predio WHERE destinacion_economica IS NOT NULL) p1
        JOIN {schema}.gc_destinacioneconomicatipo det1 ON det1.ilicode = p1.destinacion_economica
        
        UNION ALL
        
        -- Prioridad 2: Coincidencia que empiece con
        SELECT 
            det2.t_id,
            p2.destinacion_economica as p_destinacion_economica,
            2 as prioridad
        FROM (SELECT DISTINCT destinacion_economica FROM tmp_predio WHERE destinacion_economica IS NOT NULL) p2
        JOIN {schema}.gc_destinacioneconomicatipo det2 ON det2.ilicode ILIKE p2.destinacion_economica || '%'
        
        UNION ALL
        
        -- Prioridad 3: Coincidencia que contenga
        SELECT 
            det3.t_id,
            p3.destinacion_economica as p_destinacion_economica,
            3 as prioridad
        FROM (SELECT DISTINCT destinacion_economica FROM tmp_predio WHERE destinacion_economica IS NOT NULL) p3
        JOIN {schema}.gc_destinacioneconomicatipo det3 ON det3.ilicode ILIKE '%' || p3.destinacion_economica || '%'
    ) combined_matches
    ORDER BY p_destinacion_economica, prioridad
) det ON det.p_destinacion_economica = p.destinacion_economica

-- Resto de JOINs sin cambios
JOIN {schema}.gc_prediotipo tp          
    ON tp.ilicode ILIKE '%' || p.tipo_predio || '%' 
JOIN {schema}.gc_condicionprediotipo cpt          
    ON cpt.ilicode ILIKE '%' || p.condicion_predio || '%' 
JOIN {schema}.gc_estadotipo et          
    ON et.ilicode ILIKE '%Activo%' 
LEFT JOIN {schema}.gc_categoriasuelotipo cst           
    ON cst.ilicode ILIKE '%' || p.categoria_suelo || '%' 
JOIN {schema}.gc_clasesuelotipo clst            
    ON clst.ilicode ILIKE '%' || p.clase_suelo || '%' 
JOIN {schema}.col_unidadadministrativabasicatipo ubat           
    ON ubat.ilicode ILIKE '%Estadistica%';



    -- sql/inserts/01_insert_predios.sql
-- Ejemplo de insert que toma datos de la tabla temporal
-- y los inserta en la estructura final

-- Consulta para insertar datos en la tabla gc_predio desde la tabla temporal predio
-- Realiza joins con tablas de tipos para obtener los IDs correspondientes

INSERT INTO {schema}.gc_predio
(t_id, 
t_ili_tid, 
departamento, 
municipio, 
codigo_orip, 
matricula_inmobiliaria, 
numero_predial_nacional, 
codigo_homologado, 
nupre,
tipo_predio, 
condicion_predio, 
destinacion_economica, 
area_catastral_terreno, 
area_registral_m2,
vigencia_actualizacion_catastral, 
estado, 
categoria_suelo, 
clase_suelo, 
numero_predial_anterior, 
area_construida, 
nombre, 
tipo, 
comienzo_vida_util_version, 
fin_vida_util_version, 
espacio_de_nombres, 
local_id)
SELECT 
nextval('{schema}.t_ili2db_seq'::regclass), 
uuid_generate_v4(), 
departamento, 
municipio, 
codigo_orip, 
matricula_inmobiliaria::numeric,
numero_predial_nacional, 
nupre, 
nupre, 
tp.t_id as Tipo_Predio, 
cpt.t_id as condicion_Predio, 
det.t_id as destinacion_economica, 
area_catastral_terreno::numeric, 
area_registral::numeric,
vigencia_actualizacion_catastral::date, 
et.t_id as estado, 
cst.t_id as categoria_suelo, 
clst.t_id as clase_suelo, 
numero_predial_anterior, 
area_construida::numeric,
nombre, 
ubat.t_id as tipo, 
comienzo_vida_util_version::timestamp, 
fin_vida_util_version::timestamp, 
espacio_de_nombres, 
id
FROM tmp_predio p
JOIN {schema}.gc_prediotipo  tp 
        ON tp.ilicode ILIKE '%' || p.tipo_predio || '%'
JOIN {schema}.gc_condicionprediotipo cpt 
        ON cpt.ilicode ILIKE '%' || p.condicion_predio || '%'
JOIN {schema}.gc_destinacioneconomicatipo det  
        ON det.ilicode ILIKE '%' || p.destinacion_economica || '%'
JOIN {schema}.gc_estadotipo et 
        ON et.ilicode ILIKE '%Activo%'
left JOIN {schema}.gc_categoriasuelotipo cst  
        ON cst.ilicode ILIKE '%' || p.categoria_suelo  || '%'
JOIN {schema}.gc_clasesuelotipo clst   
        ON clst.ilicode ILIKE '%' || p.clase_suelo  || '%'
JOIN {schema}.col_unidadadministrativabasicatipo ubat  
        ON ubat.ilicode ILIKE '%Estadistica%';