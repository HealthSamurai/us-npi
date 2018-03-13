(ns usnpi.npi
  (:require [usnpi.db :as db]
            [clojure.string :as str])
  (:import [java.net URLEncoder]))

(defn- url-encode [x] (when x (URLEncoder/encode x)))

(defn- sanitize [x] (str/replace x #"[^a-zA-Z0-9]" ""))

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

(defn- gen-search-expression
  [fields]
  (->>
   fields
   (map (fn [[pr & pth]]
          (format "'%s:' || coalesce((resource#>>'{%s}'), '')"
                  (to-str pr) (str/join "," (mapv to-str pth)))))
   (str/join " || ' ' || \n")))

;;
;; Practitioner
;;

(def ^:private
  search-expression
  (gen-search-expression
   [[:g :name 0 :given 0]
    [:g :name 0 :given 1]
    [:m :name 0 :middle 0]
    [:p :name 0 :prefix 0]
    [:z :name 0 :siffix 0]
    [:f :name 0 :family]

    [:g :name 1 :given 0]
    [:g :name 1 :given 1]
    [:m :name 1 :middle 0]
    [:p :name 1 :prefix 0]
    [:z :name 1 :siffix 0]
    [:f :name 1 :family]

    [:s :address 0 :state]
    [:c :address 0 :city]]))

(def trgrm_idx
  (format "CREATE INDEX IF NOT EXISTS pract_trgm_idx ON practitioner USING GIST ((\n%s\n) gist_trgm_ops);"
          search-expression))

(def to-resource-expr "(resource || jsonb_build_object('id', id, 'resourceType', 'Practitioner'))")

(def practitioner-by-id-sql
  (format " select %s::text as resource from practitioner where not deleted and id = ? " to-resource-expr))

(defn get-practitioner [{{npi :npi} :route-params :as req}]
  (println "get pracititioner:" npi)
  (if-let [pr (time (:resource (first (db/query [practitioner-by-id-sql npi]))))]
    {:status 200 :body pr}
    {:status 404 :body (str "Practitioner with id = " npi " not found")}))

(defn get-practitioners-query [{q :q cnt :_count}]
  (let [cond (cond-> []
               q (into (->> (str/split q #"\s+")
                            (remove str/blank?)
                            (mapv #(format "%s ilike '%%%s%%'" search-expression %)))))]
    (format "
select jsonb_build_object('entry', jsonb_agg(row_to_json(x.*)))::text as bundle
from (select %s as resource from practitioner where not deleted %s limit %s) x"
            to-resource-expr
            (if (not (empty? cond))
              (str "\nAND\n" (str/join "\nAND\n" cond))
              "")
            (or cnt "100"))))

(defn get-pracitioners [{params :params :as req}]
  (let [q (get-practitioners-query params)
        _ (println q)
        prs (time (db/query [q]))]
    {:status 200
     :body (:bundle (first prs))}))


(defn get-practitioners-by-ids [{params :params :as req}]
  (if-let [ids  (:ids params)]
    (let [sql (format "select %s as resource from practitioner where not deleted and id in (%s)"
                      to-resource-expr
                      (->> (str/split ids #"\s*,\s*")
                           (mapv (fn [id] (str "'" (sanitize id) "'")))
                           (str/join ",")))]
      (println sql)
      {:status 200
       :body (->> (time (mapv :resource (db/query sql))))})
    {:status 422
     :body {:message "ids parameter requried"}}))

;;
;; Organizations
;;

(def ^:private
  search-expression-org
  (gen-search-expression
   [[:n :name]
    [:s :address 0 :state]
    [:c :address 0 :city]]))

(defn get-organization
  "Returns a single organization entity by its id."
  [request]
  (let [npi (-> request :route-params :npi)
        q {:select [#sql/raw "resource::text"]
           :from [:organizations]
           :where [:and [:not :deleted] [:= :id npi]]}]
    (if-let [row (first (db/query (db/to-sql q)))]
      {:status 200 :body (:resource row)}
      {:status 404 :body (format "Organization with id = %s not found." npi)})))

(defn- as-bundle
  "Composes a Bundle JSON response."
  [models]
  (format "{\"entry\": [%s]}" (str/join ",\n" (map :resource models))))

(defn get-organizations-by-ids
  "Returns multiple organization entities by their ids."
  [request]
  (if-let [ids (some->> request :params :ids parse-ids)]
    (let [q {:select [#sql/raw "resource::text"]
             :from [:organizations]
             :where [:and [:not :deleted] [:in :id ids]]}
          orgs (db/query (db/to-sql q))]
      {:status 200
       :body (as-bundle orgs)})
    {:status 400
     :body (format "Parameter ids is malformed.")}))

(defn get-organizations
  "Returns multiple organization entities by a query term."
  [request]
  (let [words (some-> request :params :q parse-words)
        limit (-> request :params :limit (or 100))

        get-raw #(db/raw (format "\n%s ilike '%%%s%%'" search-expression-org %))

        q {:select [#sql/raw "resource::text"]
           :from [:organizations]
           :where [:and [:not :deleted]]
           :limit limit}

        q (if (empty? words)
            q
            (update q :where concat (map get-raw words)))

        orgs (db/query (db/to-sql q))]

    {:status 200
     :body (as-bundle orgs)}))
