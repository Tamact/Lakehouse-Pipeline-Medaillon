-- ===========================================================================
--  PARTIE 5 - Lecture de la couche BRONZE depuis Dremio (via Nessie + MinIO)
--  Bronze = donnees FakeStoreAPI brutes + colonnes techniques de tracabilite.
-- ===========================================================================

-- 1) Produits bruts : structure imbriquee "rating" conservee telle quelle
SELECT id, title, price, category, rating, snapshot_date, ingestion_ts, _batch_id
FROM nessie.bronze.products_raw
LIMIT 20;

-- 2) Utilisateurs bruts : structures "name" et "address" non aplaties
SELECT id, email, "name", address, snapshot_date, _source_file
FROM nessie.bronze.users_raw
LIMIT 20;

-- 3) Paniers bruts : tableau "products" (liste d'articles) tel que renvoye par l'API
SELECT id, userId, "date", products, snapshot_date
FROM nessie.bronze.carts_raw
LIMIT 20;

-- 4) Volumetrie Bronze par domaine et par snapshot (preuve de l'historisation)
SELECT 'products' AS domaine, snapshot_date, COUNT(*) AS n
FROM nessie.bronze.products_raw GROUP BY snapshot_date
UNION ALL
SELECT 'users', snapshot_date, COUNT(*) FROM nessie.bronze.users_raw GROUP BY snapshot_date
UNION ALL
SELECT 'carts', snapshot_date, COUNT(*) FROM nessie.bronze.carts_raw GROUP BY snapshot_date
ORDER BY domaine, snapshot_date;
