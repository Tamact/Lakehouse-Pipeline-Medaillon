-- ===========================================================================
--  PARTIE 5.3 - JOINTURE ENTRE DOMAINES (produits x clients x paniers)
--  Exigee explicitement par le sujet, executee en direct dans Dremio.
-- ===========================================================================

-- A) Jointure "à la main" sur les 3 domaines Silver :
--    cart_items (commandes) x products (catalogue) x users (clients)
SELECT
    ci.snapshot_date,
    u.full_name                              AS client,
    u.us_region                              AS zone_client,
    p.title                                  AS produit,
    p.category                               AS categorie,
    ci.quantity,
    p.price,
    ROUND(ci.quantity * p.price, 2)          AS montant_ligne
FROM nessie.silver.cart_items  ci
JOIN nessie.silver.products    p
      ON  p.product_id   = ci.product_id
      AND p.snapshot_date = ci.snapshot_date
JOIN nessie.silver.users       u
      ON  u.user_id      = ci.user_id
      AND u.snapshot_date = ci.snapshot_date
ORDER BY ci.snapshot_date DESC, montant_ligne DESC
LIMIT 40;

-- B) Meme logique, mais Top 10 clients par chiffre d'affaires cumule (6 mois)
SELECT
    u.full_name                                   AS client,
    u.us_region,
    COUNT(DISTINCT ci.cart_id)                    AS nb_paniers,
    SUM(ci.quantity)                             AS articles_achetes,
    ROUND(SUM(ci.quantity * p.price), 2)          AS ca_total
FROM nessie.silver.cart_items ci
JOIN nessie.silver.products   p ON p.product_id = ci.product_id AND p.snapshot_date = ci.snapshot_date
JOIN nessie.silver.users      u ON u.user_id    = ci.user_id    AND u.snapshot_date = ci.snapshot_date
GROUP BY u.full_name, u.us_region
ORDER BY ca_total DESC
LIMIT 10;

-- C) Version "Gold" : la jointure est deja materialisee dans fact_cart_items
SELECT category, us_region,
       SUM(line_revenue) AS revenue,
       SUM(quantity)     AS units
FROM nessie.gold.fact_cart_items
GROUP BY category, us_region
ORDER BY revenue DESC;
