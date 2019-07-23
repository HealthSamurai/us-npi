(ns usnpi.warmup
  "A module with a task that restores cache blocks into PG's memory.
  See http://raghavt.blogspot.ru/2014/06/utilising-caching-contribs-pgprewarm.html"
  (:require [usnpi.db :as db]))

(defn db-settings
  []
  (first (db/query "show shared_buffers")))

(defn cache-stats
  []
  (db/query
   "
SELECT
  c.relname, count(*) AS buffers
FROM
  pg_buffercache b
INNER JOIN pg_class c
  ON b.relfilenode = pg_relation_filenode(c.oid)
     AND b.reldatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
GROUP BY c.relname
ORDER BY 2 DESC
LIMIT 10;
"))

(defn task-warmup-index
  []
  (db/query "select pg_prewarm('pract_given_sort')")
  (db/query "select pg_prewarm('org_name_sort')")
  (db/query "select pg_prewarm('pract_trgm_idx')")
  (db/query "select pg_prewarm('org_trgm_idx')")
  (db/query "select pg_prewarm('pract_city_trgm_idx')")
  (db/query "select pg_prewarm('pract_state_trgm_idx')")
  (db/query "select pg_prewarm('pract_zip_trgm_idx')")
  (db/query "select pg_prewarm('pract_given_trgm_idx')")
  (db/query "select pg_prewarm('pract_family_trgm_idx')")
  (db/query "select pg_prewarm('org_name_trgm_idx')")
  (db/query "select pg_prewarm('org_city_trgm_idx')")
  (db/query "select pg_prewarm('org_state_trgm_idx')")
  (db/query "select pg_prewarm('org_zip_trgm_idx')")
  (db/query "select pg_prewarm('idxprcgiven')")
  (db/query "select pg_prewarm('idxprcfamily')")
  (db/query "select pg_prewarm('idxprcspec')")
  (db/query "select pg_prewarm('idxorgname')")
  (db/query "select pg_prewarm('idxorgspec')")
  (db/query "select pg_prewarm('idxprczip')")
  (db/query "select pg_prewarm('idxorgzip')")
  (db/query "select pg_prewarm('idxprccity')")
  (db/query "select pg_prewarm('idxorgcity')")
  (db/query "select pg_prewarm('idxprcstate')")
  (db/query "select pg_prewarm('idxorgstate')")
  nil)
