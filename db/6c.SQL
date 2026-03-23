-- calcula media e desvio padrao por commodity
WITH stats AS (
    SELECT
            id_commodity
            ,AVG(val_brl) AS media
            ,STDDEV(val_brl) AS desvio
    FROM analytics.fato_preco   
    -- considera apenas valores validos
    WHERE val_brl > 0
    GROUP BY id_commodity
)
-- identifica anomalias com base em desvio da media
SELECT
        c.nome AS commodity
        ,r.nome AS regiao
        ,f.dt_ref
        ,f.val_brl
        ,ROUND(s.media::numeric,  2) AS media_historica
        ,ROUND(s.desvio::numeric, 2) AS desvio_padrao

        -- classifica tipo de anomalia
        ,CASE
            WHEN f.val_brl <= 0
                THEN 'preço negativo ou zero'
            WHEN f.val_brl > s.media + 3 * s.desvio
                THEN 'acima de 3 desvios'
            WHEN f.val_brl < s.media - 3 * s.desvio
                THEN 'abaixo de 3 desvios'
        END AS tipo_anomalia

FROM analytics.fato_preco f
-- relaciona dimensoes e estatisticas
JOIN analytics.dim_commodity c
        ON c.id_commodity = f.id_commodity
JOIN analytics.dim_regiao r
        ON r.id_regiao = f.id_regiao
JOIN stats s
        ON s.id_commodity = f.id_commodity
-- filtra apenas registros anomalos
WHERE
    f.val_brl <= 0
    OR f.val_brl > s.media + 3 * s.desvio
    OR f.val_brl < s.media - 3 * s.desvio
-- ordena resultado final
ORDER BY c.nome, f.dt_ref;