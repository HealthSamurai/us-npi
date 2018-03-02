(ns usnpi.error
  "A set of wrappers for exceptions.")

(defn error!
  ([msg]
   (throw (Exception. msg)))
  ([tpl & args]
   (error! (apply format tpl args))))
