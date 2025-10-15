(ns project_x.core
  (:require
    [project_x.data :as data]
    )
  )

(defn -main []
  (println "Let's go!")
  (data/get-income-statement "AAPL")
  (println "Ready!")
  )
