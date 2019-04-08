(ns usnpi.npi
  (:require [usnpi.db :as db]
            [clojure.string :as str]
            [usnpi.aidbox :refer [->aidbox]]
            [usnpi.http :as http]
            [cheshire.core :as json]))

(def sql-limit 100)

;;
;; Helpers
;;

(defn- parse-ids
  [ids-str]
  (not-empty (re-seq #"\d+" ids-str)))

(defn- parse-words
  [term]
  (not-empty
   (as-> term $
     (str/split $ #"\s+")
     (remove empty? $))))

(defn- to-str
  [x]
  (if (keyword? x) (name x) (str x)))

(defn as-bundle
  "Composes a Bundle JSON response from a list of JSON strings."
  [models]
  {:entry models})

(defn- gen-search-expression
  [fields]
  (->>
   fields
   (map (fn [[pr & pth]]
          (format "'%s:' || coalesce((resource#>>'{%s}'), '')"
                  (to-str pr) (str/join "," (mapv to-str pth)))))
   (str/join " || ' ' || \n")))

(defn request-opt
  [request]
  {:aidbox? (some-> request :params :aidbox not-empty boolean)})

(defn process-model
  [model & [opt]]
  (cond-> model
    (:aidbox? opt)
    (update :resource ->aidbox)))

(defn process-models
  [models & [opt]]
  (for [model models]
    (process-model model opt)))

;;
;; Practitioner
;;

(def ^:private
  sql-like-clause-pract
  (gen-search-expression
   [[:g :name 0 :given 0]
    [:g :name 0 :given 1]
    [:p :name 0 :prefix 0]
    [:z :name 0 :suffix 0]
    [:f :name 0 :family]

    [:g :name 1 :given 0]
    [:g :name 1 :given 1]
    [:p :name 1 :prefix 0]
    [:z :name 1 :suffix 0]
    [:f :name 1 :family]

    [:s :address 0 :state]
    [:c :address 0 :city]

    [:zip :address 0 :postalCode]]))

(def field-queries
  {"zip" "resource#>>'{address,0,postalCode}'"
   "c" "resource#>>'{address,0,city}'"
   "s" "resource#>>'{address,0,state}'"
   "g" "(coalesce((resource#>>'{name,0,given,0}'), '') || ' '
|| coalesce((resource#>>'{name,0,given,1}'), '') || ' '
|| coalesce((resource#>>'{name,1,given,0}'), '') || ' '
|| coalesce((resource#>>'{name,1,given,1}'), ''))"
   "p" "(coalesce(resource#>>'{name,0,prefix,0}'), '') || ' '
|| coalesce((resource#>>'{name,0,prefix,1}'), '') || ' '
|| coalesce((resource#>>'{name,1,prefix,0}'), '') || ' '
|| coalesce((resource#>>'{name,1,prefix,1}'), '')"
   "z" "(coalesce((resource#>>'{name,0,suffix,0}'), '') || ' '
|| coalesce((resource#>>'{name,1,suffix,0}'), ''))"
   "f" "(coalesce((resource#>>'{name,0,family}'), '') || ' '
|| coalesce((resource#>>'{name,1,family}'), ''))"})

(defn get-like-parameters [term]
  (let [[f s :as parts] (str/split term #"\:" 2)]
    (if (= 2 (count parts))
      (let [query (get field-queries f)
            prefix (if (and query
                            (= "(coalesce" (subs query 0 9)))
                     "%" "")]
        [query prefix s])
      [sql-like-clause-pract "%" term])))

(defn sql-like-pract [term]
  (let [[clause prefix term] (get-like-parameters term)]
    (db/raw (format "%s ilike '%s%s%%'" clause prefix term))))

(defn sql-like-pract-with-or [term]
  (let [parts (str/split term #"\|")]
    (if (< 1 (count parts))
      (into [:or] (mapv sql-like-pract parts))
      (sql-like-pract term))))

(def trgrm_idx
  (format "CREATE INDEX IF NOT EXISTS pract_trgm_idx ON practitioner USING GIST ((\n%s\n) gist_trgm_ops);"
          sql-like-clause-pract))

(def query-practitioner
  {:select [:resource]
   :from [:practitioner]
   :where [:and [:not :deleted]]})

(defn get-practitioner
  [request]
  (let [npi (-> request :params :npi)
        q (update query-practitioner :where conj [:= :id npi])
        opt (request-opt request)]
    (if-let [model (first (db/query (db/to-sql q)))]
      (http/http-resp (:resource (process-model model opt)))
      (http/err-resp 404 "Practitioner with id = %s not found." npi))))

(defn get-practitioners-by-ids
  [request]
  (if-let [ids (some->> request :params :ids parse-ids)]
    (let [q (update query-practitioner :where conj [:in :id ids])
          models (db/query (db/to-sql q))
          opt (request-opt request)]
      (http/http-resp (as-bundle (process-models models opt))))
    (http/err-resp 400 "Parameter ids is malformed.")))

(defn get-practitioners
  [request]
  (let [words (some-> request :params :q parse-words)
        limit (-> request :params :_count (or sql-limit))
        opt (request-opt request)

        q (assoc query-practitioner :limit limit)

        q (if-not (empty? words)
            (update q :where concat (map sql-like-pract-with-or words))
            q)
        models (db/query (db/to-sql q))]

    (http/http-resp (as-bundle (process-models models opt)))))

;;
;; Organizations
;;

(def ^:private
  sql-like-clause-org
  (gen-search-expression
   [[:n :name]
    [:s :address 0 :state]
    [:c :address 0 :city]
    [:zip :address 0 :postalCode]]))

(def org-field-queries
  {"zip" "resource#>>'{address,0,postalCode}'"
   "c" "resource#>>'{address,0,city}'"
   "s" "resource#>>'{address,0,state}'"
   "n" "resource#>>'{name}'"})

(def ^:private
  query-organization
  {:select [:resource]
   :from [:organizations]
   :where [:and [:not :deleted]]})

(defn get-like-parameters-org [term]
  (let [[f s :as parts] (str/split term #"\:" 2)]
    (if (= 2 (count parts))
      [(get org-field-queries f) "" s]
      [sql-like-clause-org "%" term])))

(defn sql-like-org [term]
  (let [[clause prefix term] (get-like-parameters-org term)]
    (db/raw (format "%s ilike '%s%s%%'" clause prefix term))))

(defn sql-like-org-with-or [term]
  (let [parts (str/split term #"\|")]
    (if (< 1 (count parts))
      (into [:or] (mapv sql-like-org parts))
      (sql-like-org term))))

(defn get-organization
  "Returns a single organization entity by its id."
  [request]
  (let [npi (-> request :params :npi)
        q (update query-organization :where conj [:= :id npi])
        opt (request-opt request)]
    (if-let [model (first (db/query (db/to-sql q)))]
      (http/http-resp (:resource (process-model model opt)))
      (http/err-resp 404 "Organization with id = %s not found." npi))))

(defn get-organizations-by-ids
  "Returns multiple organization entities by their ids."
  [request]
  (if-let [ids (some->> request :params :ids parse-ids)]
    (let [q (update query-organization :where conj [:in :id ids])
          models (db/query (db/to-sql q))
          opt (request-opt request)]
      (http/http-resp (as-bundle (process-models models opt))))
    (http/err-resp 400 "Parameter ids is malformed.")))

(defn get-organizations
  "Returns multiple organization entities by a query term."
  [request]
  (let [words (some-> request :params :q parse-words)
        limit (-> request :params :_count (or sql-limit))
        opt (request-opt request)

        q (assoc query-organization :limit limit)

        q (if-not (empty? words)
            (update q :where concat (map sql-like-org-with-or words))
            q)

        models (db/query (db/to-sql q))]

    (http/http-resp (as-bundle (process-models models opt)))))
