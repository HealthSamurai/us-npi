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
  (first (db/query "select pg_prewarm('pract_trgm_idx')")))
