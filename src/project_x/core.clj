(ns project_x.core
  (:require
    [project_x.data :as data]
    )
  )

(defn -main []
  (println "Start!")
  (data/save-clean-data!)
  (println "End!")
  )
