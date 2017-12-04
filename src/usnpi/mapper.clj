(ns usnpi.mapper
  (:require [clojure.string :as str]))

(defn match [pat obj]
  (cond
    (vector? pat) (when (vector? obj)
                    (loop [[p & ps] pat
                           [o & os] obj]
                      (if (and (nil? o) (nil? p))
                        true
                        (if (match p o)
                          (recur ps os)
                          false))))
    (map? pat) (when (map? obj)
                 (reduce (fn [acc [pk pv]]
                           (and acc (match pv (get obj pk))))
                         true pat))
    :else (= pat obj)))

(defn extract [obj rule]
  (loop [[r & rs] rule obj obj]
    (cond
      (nil? r) obj
      (or (string? r) (keyword? r)) (when-let [next (get obj r)] (recur rs next))
      (number? r) (when-let [next (get obj r)] (recur rs next))
      (and (map? r) (vector? obj) (:$collection r) (:$filter r)) (recur rs (filterv #(match (:$filter r) %) obj))
      (and (map? r) (vector? obj) (:$collection r)) (recur rs obj)
      (and (map? r) (vector? obj)) (recur rs (first (filterv #(match r %) obj)))
      :else nil)))

(defn inject [obj [r & rs] val]
  (when-not (nil? val)
    (cond
      (and (nil? r) (nil? r)) val
      (and (map? obj) (keyword? r))
      (assoc obj r (inject (get obj r) rs val))

      (and (vector? obj) (= [] r))
      (conj obj val)

      (and (nil? obj) (= [] r))
      [val]
      

      (and (nil? obj) (keyword? r))
      (assoc {} r (if (empty? rs)
                    val
                    (inject (get obj r) rs val)))

      (and (nil? obj) (nil? r)) val

      (and (vector? obj) (number? r))
      (if (< r (count obj))
        (assoc obj r (inject (get obj r) rs val))
        (conj (into obj (vec (repeat (- (dec (count obj)) r) nil))) (inject (get obj r) rs val)))

      (and (nil? obj) (number? r))
      (if (< r 0)
        [(inject nil rs val)]
        (assoc (vec (repeat r nil)) r (inject nil rs val)))

      (and (map? r) (:$collection r) (vector? obj))
      (into obj val)

      (and (map? r) (:$collection r) (nil? obj))
      val

      (and (map? r) (vector? obj))
      (if (some #(match r %) obj)
        (reduce (fn [acc x]
                  (if (match r x)
                    (conj acc (inject x rs val))
                    (conj acc x)))
                [] obj)
        (conj obj (inject r rs val)))

      (and (nil? obj) (map? r))
      [(inject r rs val)]

      :else  (throw (Exception. (pr-str "Inject?" [obj r rs val]))))))

(defn build-name [& parts]
  (keyword (str/join "_" (map name parts))))

(defn transform [obj mapping [from-key to-key]]
  (reduce
   (fn [acc rule]
     (let [to-rule   (get rule to-key)
           from-rule (get rule from-key)
           to-const  (get rule (build-name to-key "const"))
           to-fn     (get rule (build-name "to" to-key))
           val       (cond to-const to-const
                           from-rule  (extract obj from-rule)
                           (:calculate rule) (extract acc (:calculate rule)))
           val (if (and val to-fn) (to-fn val) val)
           val (if-let [m (and val (vector? val) (:$mapping rule))]
                 (mapv (fn [v]
                        (when-let [mval (transform v m [from-key to-key])]
                          (if-let [flt (:$filter (last to-rule))]
                            (merge flt mval)
                            mval)))
                      val)
                 val)]
       (if (and val to-rule) (inject acc to-rule val) acc)))
   {} mapping))
