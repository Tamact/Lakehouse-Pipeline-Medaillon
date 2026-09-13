-- ===========================================================================
--  BONUS Partie 7 - Fonctionnalites Iceberg / Nessie avancees depuis Dremio
-- ===========================================================================

-- 1) Historique des snapshots Iceberg d'une table (metadonnee "table$snapshots")
SELECT *
FROM TABLE(table_snapshot('nessie.gold.agg_sales_by_category_month'));

-- 2) Time travel Iceberg : etat de la table a un instant / snapshot donne
--    (remplacer l'ID par une valeur vue a la requete 1)
-- SELECT COUNT(*) FROM nessie.gold.fact_cart_items
--   AT SNAPSHOT '<snapshot_id>';
-- SELECT COUNT(*) FROM nessie.gold.fact_cart_items
--   AT TIMESTAMP '2026-09-01 00:00:00';

-- 3) Versionnement Nessie : interroger une autre branche du catalogue
--    (le pipeline peut ecrire dans une branche de travail avant merge sur main)
-- SELECT COUNT(*) FROM nessie.bronze.products_raw AT BRANCH "etl_run_2026_09_07";

-- 4) Evolution de schema : ajout de colonne visible sans reecriture des donnees
-- ALTER TABLE nessie.silver.products ADD COLUMNS (margin_estimee DOUBLE);
-- SELECT product_id, price, margin_estimee FROM nessie.silver.products LIMIT 5;
