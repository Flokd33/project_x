(defproject project_x "0.1.0-SNAPSHOT"
  :description "case study"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [clj-http "3.12.3"]              ;HTTP request
                 [metosin/jsonista "0.3.7"]       ;JSON
                 [techascent/tech.ml.dataset "7.062"] ;dataset

                 ]
  :main project_x.core
  :repl-options {:init-ns project_x.core})
