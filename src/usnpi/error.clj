(ns usnpi.error
  "A set of wrappers for exceptions.")

(defn error!
  ([msg]
   (throw (Exception. msg)))
  ([tpl & args]
   (error! (apply format tpl args))))

(defn ^String exc-msg
  "Returns a message string for an exception instance."
  [^Exception e]
  (let [class (-> e .getClass .getCanonicalName)
        message (-> e .getMessage (or "<no message>"))]
    (format "Exception: %s %s" class message)))

(defmacro recover
  "Returns the value if any error occurs in the body."
  [value & body]
  `(try
     ~@body
     (catch Throwable e#
       ~value)))
