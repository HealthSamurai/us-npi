(ns usnpi.search
  (:require [usnpi.http :as http]
            [usnpi.db :as db]
            [usnpi.npi :as npi]
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
    city         (conj [:ilike (resource :address :city) (str "%" city "%")])
    state        (conj [:=     (resource :address :state) state])
    postal-codes (conj [:in    (resource :address :postalCode) postal-codes])))

(defn with-count [select {:keys [count]}]
  (cond-> select
    count (assoc :limit count)))

(defn only-organization? [{:keys [name org first-name last-name]}]
  (and org (not name) (not first-name) (not last-name)))

(defn only-practitioner? [{:keys [name org first-name last-name]}]
  (and (or first-name last-name) (not name) (not org)))

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
                    :always      (build-where params))
           :order-by [(resource :name 0 :family)]}
          (with-count params)))))

(defn build-organization-sql [{:keys [name org count] :as params}]
  (when-not (only-practitioner? params)
    (let [org (or name org)]
      (-> {:select [:id :resource]
           :from [:organizations]
           :where (cond-> [:and [:= :deleted false]]
                    org          (conj [:ilike (resource :name) (str "%" org "%")])
                    :always      (build-where params))
           :order-by [(resource :name)]}
          (with-count params)))))

(defn search [{params :params}]
  (let [practitioner-sql (build-practitioner-sql params)
        organization-sql (build-organization-sql params)
        sql (cond
              (and practitioner-sql organization-sql) {:union-with-parens [practitioner-sql organization-sql]}
              practitioner-sql                        practitioner-sql
              organization-sql                        organization-sql)]
    (http/http-resp (npi/as-bundle (db/query (db/to-sql sql))))))
