(ns usnpi.util
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [org.httpkit.client :as http]
            [clojure.java.shell :as sh]
            [cheshire.core :as json]
            [clojure.string :as str])
  (:import java.util.regex.Matcher))

(defn to-json [x]
  (json/encode x))

(defn from-json [x]
  (json/decode x keyword))

(def base-dir
  (or (-> env :fhirterm-base not-empty)
      "FHIRTERM_BASE"))

(def ^:dynamic *wd* base-dir)

(defmacro in-dir [dir & body]
  `(binding [*wd* (from-workdir ~dir)]
     (mk-dir "")
     ~@body))

(defn from-workdir [path]
  (str *wd* (when (and (not (str/ends-with? *wd* "/"))
                       (not (str/starts-with? path "/")))
              "/") path))

(defn exists? [path]
  (.exists (io/file (from-workdir path))))

(defn mk-dir [path]
  (sh/sh "mkdir" "-p" (from-workdir path)))

(defn spit* [path content]
  (spit (from-workdir path) content))

(defn slurp* [path]
  (slurp (from-workdir path)))

(defn exec! [cmd]
  (log/info "Execute:" cmd)
  (let [cmd cmd
        res (sh/sh "bash" "-c" cmd)]
    (when-not (= 0 (:exit res))
      (throw (Exception. (pr-str res))))))

(defn exec-in-current-dir! [cmd]
  (log/info "Execute:" cmd)
  (let [cmd cmd
        res (sh/sh "bash" "-c" cmd :dir *wd*)]
    (when-not (= 0 (:exit res))
      (throw (Exception. (pr-str res))))))

(defn match-and-replace
  [s re replacement]
  (let [buf (StringBuffer.)
        m (re-matcher re s)]
    (loop [res []]
      (if (.find m)
        (do
          (.appendReplacement m buf (Matcher/quoteReplacement replacement))
          (recur (conj res (.group m 1))))
        (do
          (.appendTail m buf)
          [(str buf) res])))))

(defn sql-match-and-replace
  [s re replacement]
  (let [buf (StringBuffer.)
        m (re-matcher re s)]
    (loop [res []]
      (if (.find m)
        (do
          (.appendReplacement m buf (Matcher/quoteReplacement replacement))
          (recur (conj res [(.group m 1) (.group m 2)])))
        (do
          (.appendTail m buf)
          [(str buf) res])))))

(defn to-pg-array [x]
  (if (and x (not (empty? x)))
    (str "ARRAY[" (str/join "," (map #(str "$TXT$" % "$TXT$") x)) "]")
    "NULL"))

(defn to-pg-jsonb [x]
  (if (and x (not (empty? x)))
    (str "$JSON$" (to-json x) "$JSON$")
    "NULL"))

(defmacro sql-template
  "
  (let [ss \"string\"
       raw \"select *\"
       has {:a 1}]
   (sql-template \"~(raw) ~s(ss) ~j(has)\"))
  => select * 'string' $JSON${\"a\":1}$JSON$
  "
  [s]
  (let [[s' format-replacements] (sql-match-and-replace s #"~([sja]?)\(([\w.*+!?$%&=<>'-]+)\)" "%s")
        fmt-args (map (fn [[tp sym]]
                        (cond
                          (= tp "") (symbol sym)
                          (= tp "s") (list 'str "$TXT$" (symbol sym) "$TXT$")
                          (= tp "a") (list 'terminology.util/to-pg-array (symbol sym))
                          (= tp "j") (list 'terminology.util/to-pg-jsonb (symbol sym))
                          :else (throw (Exception. (str "Do not know how to render " tp)))
                          )
                        ) format-replacements)]
    `(format ~s' ~@fmt-args)))

(defmacro str-template
  "Replaces ~(foo) to value of `foo`.
  (sql-string \"SELECT * FROM ~(table) \") =>
  (format \"SELECT * FROM %s\" table)"
  [s]
  (let [[s' format-replacements] (match-and-replace s #"~\(([\w.*+!?$%&=<>'-]+)\)" "%s")]
    `(format ~s' ~@(map symbol format-replacements))))

(defn curl
  ([url output]
   (let [output (from-workdir output)]
     (-> (str-template
          "curl '~(url)' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Upgrade-Insecure-Requests: 1'  -H 'Connection: keep-alive' --compressed -o ~(output)")
         exec!)))
  ([cookies url output]
   (let [output (from-workdir output)]
     (-> (str-template
          "curl '~(url)' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Upgrade-Insecure-Requests: 1'  -H 'Connection: keep-alive' -H 'Cookie: ~(cookies)'  --compressed -o ~(output)")
         exec!))))

(defn unzip [path]
  (log/info "unzip" (from-workdir path))
  (sh/sh "unzip" path :dir *wd*))

(defn normaliza-column-name [x]
  (-> x
      str/lower-case
      (str/replace #"\s+" "_")
      (str/replace #"[^a-z0-9_]" "")))

(defn table-def-from-csv [name path & [{:keys [delim temp?]}]]
  (with-open [in-file (io/reader (from-workdir path))]
    (let [headers (str/split (first (line-seq in-file)) (or delim #","))
          ddl-columns (map (fn [x] (str (normaliza-column-name x) " text")) headers)]
      (str "CREATE " (when temp? " TEMP ") " TABLE " name " (\n" (str/join ",\n " ddl-columns) "\n);"))))

(defn xlsx-to-csv [source dest ]
  (exec! (format "ssconvert %s %s -S" (from-workdir source) (from-workdir dest))))

(defn file-seq* [path]
  (file-seq (io/file (from-workdir path))))

(defn find-file [path re]
  (some
   (fn [file]
     (let [path (.getAbsolutePath file)]
       (when (re-find re path)
         path)))
   (file-seq* path)))

(defn init
  "Creates the base dir if it does not exist."
  []
  (if (-> base-dir io/file .exists)
    (log/infof "Your base dir is %s" base-dir)
    (do
      (log/infof "Dir %s doesn't exist, creating it" base-dir)
      (sh/sh "mkdir" "-p" base-dir))))
