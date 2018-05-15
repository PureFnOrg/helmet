(defproject org.purefn/helmet "0.1.0-SNAPSHOT"
  :description "A simple componentized http elastic search library."
  :url "https://github.com/PureFnOrg/helmet"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [com.stuartsierra/component "0.3.2"]
                 [cheshire "5.7.0"]
                 [clj-http "2.3.0"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [com.stuartsierra/component.repl "0.2.0"]]
                   :source-paths ["dev"]}})
