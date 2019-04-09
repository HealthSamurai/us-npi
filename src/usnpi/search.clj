(ns usnpi.search
  (:require [usnpi.http :as http]
            [usnpi.db :as db]
            [usnpi.npi :as npi]
            [honeysql.format :as sqlf]
            [clojure.string :as str]))

(defmacro resource [& fields]
  (let [s (str "resource#>>'{"
               (str/join "," (map (fn [field]
                                    (if (keyword field)
                                      (name field)
                                      field))
                                  fields))
               "}'")]
    `(db/raw ~s)))

(defn build-where [where {:keys [postal-codes state city]}]
  (cond-> where
    city         (conj [:ilike (resource :address 0 :city) (str "%" city "%")])
    state        (conj [:=     (resource :address 0 :state) state])
    postal-codes (conj [:in    (resource :address 0 :postalCode) postal-codes])))

(defn only-organization? [{:keys [name org first-name last-name]}]
  (and org (not name) (not first-name) (not last-name)))

(defn only-practitioner? [{:keys [name org first-name last-name]}]
  (and (or first-name last-name) (not name) (not org)))

(defn with-count [query {:keys [count]}]
  (cond-> query
    count (assoc :limit count)))

(defn with-order [query params pred col type ]
  (if (pred params)
    (assoc query :order-by [col])
    (update query :select conj [col :name] [type :type])))

(defn build-practitioner-sql [{:keys [name first-name last-name taxonomies] :as params}]
  (when-not (only-organization? params)
    (let [family     (or name last-name)
          first-name (and (not name) first-name)]
      (-> {:select [:id :resource]
           :from [:practitioner]
           :where (cond-> [:and [:= :deleted false]]
                    family       (conj [:ilike (resource :name 0 :family) (str family "%")])
                    first-name   (conj [:ilike (resource :name 0 :family) (str "%" first-name "%")])
                    taxonomies   (conj [:in    (resource :qualification 0 :code :coding 0 :code) taxonomies])
                    :always      (build-where params))}
          (with-count params)
          (with-order params only-practitioner? (resource :name 0 :family) 1)))))

(defn build-organization-sql [{:keys [name org count] :as params}]
  (when-not (only-practitioner? params)
    (let [org (or name org)]
      (-> {:select [:id :resource]
           :from [:organizations]
           :where (cond-> [:and [:= :deleted false]]
                    org          (conj [:ilike (resource :name) (str "%" org "%")])
                    :always      (build-where params))}
          (with-count params)
          (with-order params only-organization? (resource :name) 2)))))

(defmethod sqlf/format-clause :union-practitioner-and-organization [[_ [left right]] _]
  (str "(" (sqlf/to-sql left) ") union all (" (sqlf/to-sql right) ")"))

(defn union [practitioner-sql organization-sql]
  {:select [:id :resource]
   :from [[{:union-practitioner-and-organization [practitioner-sql
                                                 organization-sql]} :q]]
   :order-by [:type :name]})

(defn as-vector [s]
  (when-not (str/blank? s)
    (str/split s #",")))

#_(db/query (db/to-sql {:select [(resource :address 0 :postalCode)]
                      :from [:practitioner]
                      :limit 3}))

(defn search [{params :params}]
  (let [params (-> params
                   (update :postal-codes as-vector)
                   (update :taxonomies as-vector))
        p-sql (build-practitioner-sql params)
        o-sql (build-organization-sql params)
        sql (cond
              (and p-sql o-sql) (union p-sql o-sql)
              p-sql             p-sql
              o-sql             o-sql)]
    (http/http-resp (npi/as-bundle (db/query (db/to-sql sql))))))
