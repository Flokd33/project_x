(ns project_x.tools
  (:require [jsonista.core :as jsonista]
            ;[server.static :as static]
            ;[clojure.java.shell :refer [sh]]
            ;[clojure.string :as str]
            ;[clojure.java.io :as io]
            )
  ;(:import (java.util.zip ZipOutputStream ZipInputStream)
  ;         (java.io PrintStream)
  ;         (java.time.format DateTimeFormatter DateTimeParseException)
  ;         (java.time ZonedDateTime ZoneId LocalDate Period YearMonth)
  ;         (java.util Date)
  ;         (java.time.temporal ChronoUnit)
  ;         (java.util Locale)
  ;         )
  )

(defn json->kmap [^String s] (jsonista/read-value s (jsonista/object-mapper {:decode-key-fn true})))