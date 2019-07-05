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

(defn only-organization? [{:keys [name org first-name last-name taxonomies]}]
  (and org (not name) (not first-name) (not last-name) (not taxonomies)))

(defn only-practitioner? [{:keys [name org first-name last-name taxonomies]}]
  (and (or first-name last-name taxonomies) (not name) (not org)))

(defn with-count [query {:keys [count]}]
  (cond-> query
    count (assoc :limit count)))

(defn with-type [query params pred type]
  (if (pred params)
    query
    (update query :select conj [type :type])))

(defn build-practitioner-sql [{:keys [name first-name last-name taxonomies] :as params}]
  (when-not (only-organization? params)
    (let [family     (or name last-name)
          first-name (and (not name) first-name)
          name-col   (resource :name 0 :family)]
      (-> {:select [:id :resource [name-col :name]]
           :from [:practitioner]
           :where (cond-> [:and [:= :deleted false]]
                    family       (conj [:ilike name-col (str family "%")])
                    first-name   (conj [:ilike (resource :name 0 :given) (str "%" first-name "%")])
                    taxonomies   (conj [:in    (resource :qualification 0 :code :coding 0 :code) taxonomies])
                    :always      (build-where params))}
          (with-count params)
          (with-type params only-practitioner? 1)))))

(defn build-organization-sql [{:keys [name org count] :as params}]
  (when-not (only-practitioner? params)
    (let [org (or name org)
          name-col (resource :name)]
      (-> {:select [:id :resource [name-col :name]]
           :from [:organizations]
           :where (cond-> [:and [:= :deleted false]]
                    org          (conj [:ilike name-col (str "%" org "%")])
                    :always      (build-where params))}
          (with-count params)
          (with-type params only-organization? 2)))))

(defn wrap-query [query]
  {:select [:id :resource]
   :from [[query :q]]
   :order-by [:name]})

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

(defn search [{params :params}]
  (let [params (-> params
                   (update :postal-codes as-vector)
                   (update :taxonomies as-vector))
        p-sql (build-practitioner-sql params)
        o-sql (build-organization-sql params)
        sql (cond
              (and p-sql o-sql) (union p-sql o-sql)
              p-sql             (wrap-query p-sql)
              o-sql             (wrap-query o-sql))]
    (http/http-resp (npi/as-bundle (db/query (db/to-sql sql))))))
