-- stg_catalogue__products: one row per product in the catalogue in
-- force. SKUs are written as Tradeline writes them; the supplier is
-- procurement's supplier_id, through the seed procurement keeps.
SELECT
    upper(trim(c."SKU"))                            AS sku,
    trim(c."Product Name")                          AS product_name,
    c."Category"                                    AS category,
    trim(c."Supplier")                              AS supplier_name,
    n.supplier_id::INTEGER                          AS supplier_id,
    CAST(CAST(replace(c."List Price", '$', '') AS DECIMAL(12, 2)) * 100
         AS BIGINT)                                 AS list_price_cents,
    CAST(strptime(nullif(c."Launched", ''), '%d-%b-%Y') AS DATE)
                                                    AS launched_date,
    nullif(trim(c."Notes"), '')                     AS notes,
    c._load_id
FROM {{ source('raw', 'catalogue_products') }} AS c
SEMI JOIN {{ source('audit', 'current_files') }} AS f
    ON f.source = 'catalogue' AND f.name = c.file
   AND f.sha256 = c.file_sha256
LEFT JOIN {{ ref('supplier_names') }} AS n
    ON n.source = 'catalogue' AND n.spelling = trim(c."Supplier")
