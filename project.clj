(defproject pbf-reader "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.flatland/protobuf "0.8.1"]
                 [clj-configurator "0.1.5"]
                 [scrosby/osmpbf "1.3.1"]]
  :plugins [[lein-protobuf "0.1.1"]]
  :main pbf-reader.core
  :profiles {:dev {:dependencies [[spyscope "0.1.4"]]}}
  :aot :all)
