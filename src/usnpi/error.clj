(ns usnpi.error
  "A set of wrappers for exceptions.")

(defn error!
  ([msg]
   (throw (Exception. msg)))
  ([tpl & args]
   (error! (apply format tpl args))))

(defn- shrink
  [str limit]
  (if (> (count str) limit)
    (subs str 0 limit)
    str))

(defn ^String exc-msg
  "Returns a message string for an exception instance."
  [^Exception e]
  (let [class (-> e .getClass .getCanonicalName)
        message (-> e .getMessage (or "<no message>"))]
    (shrink (format "%s: %s" class message) 255)))
