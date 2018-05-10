(ns org.purefn.helmet.http-elasticsearch
  (:refer-clojure :exclude (alias type))
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.set :refer [rename-keys]]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [org.purefn.helmet.protocol :as proto]))

(defn- gen-url
  [trail? & args]
  (cond-> (str/join "/" (filter some? args))
    trail? (str "/")))

(defrecord HTTPElasticsearch [config]

  proto/Elasticsearch
  (create-index [_ index opts]
    (let [{:keys [mappings
                  settings
                  shards
                  replicas]} opts
          index-args         (cond-> {}
                               mappings (assoc :mappings mappings)
                               settings (assoc :settings settings)
                               replicas (assoc-in [:settings :number_of_replicas] replicas)
                               shards   (assoc-in [:settings :number_of_shards] shards))]
      (http/put (gen-url true (::url config) index)
                {:content-type :json
                 :form-params  index-args
                 :insecure?    true})
      true))

  (index-info [_ index]
    (try
      (-> (http/get (gen-url false (::url config) index)
                    {:insecure? true})
          :body
          (json/parse-string true))
      (catch Exception e
        (if (= 404 (:status (ex-data e)))
          nil
          (throw e)))))

  (delete-index [_ index]
    (http/delete (gen-url true (::url config) index)
                 {:insecure? true})
    true)

  (alias-indices [_ alias indices]
    (http/post (gen-url false (::url config) "aliases")
               {:content-type :json
                :form-params  {:actions [{:remove {:index "*"
                                                   :alias alias}}
                                         {:add {:indices indices
                                                :alias   alias}}]}
                :insecure?    true})
    true)

  (get-indices [_ alias]
    (some-> (http/get (gen-url false (::url config) "_alias" alias)
                      {:insecure? true})
          (:body)
          (json/parse-string)
          (keys)))

  (add-document [this index id document]
    (proto/add-document this index nil id document))
  (add-document [_ index type id document]
    (http/put (gen-url false (::url config) index type id)
              {:content-type :json
               :form-params  document
               :insecure?    true})
    true)

  (bulk-add-documents [this index documents]
    (proto/bulk-add-documents this index nil documents))
  (bulk-add-documents [_ index type documents]
    (let [data     (->> documents
                        (mapcat (fn [[id document]]
                                  [{:index {:_id id}}
                                   document]))
                        (map json/generate-string))
          data-str (-> data
                       (interleave (repeat "\n"))
                       (str/join))
          response (http/post (gen-url false (::url config) index type "_bulk")
                              {:content-type :json
                               :body         data-str
                               :insecure?    true})
          body     (json/parse-string (:body response) true)]
      (zipmap (map first documents)
              (map #(or (:error (:index %)) true)
                   (:items body)))))

  (update-document [this index id delta]
    (proto/update-document this index nil id delta))
  (update-document [_ index type id delta]
    (http/post (gen-url false (::url config) index type id "_update")
               {:content-type :json
                :form-params  {:doc delta}
                :insecure?    true})
    true)

  (delete-document [this index id]
    (proto/delete-document this index nil id))
  (delete-document [_ index type id]
    (http/delete (gen-url false (::url config) index type id)
                 {:insecure? true})
    true)

  (get-document [this index id]
    (proto/get-document this index nil id))
  (get-document [_ index type id]
    (some-> (http/get (gen-url false (::url config) index type id))
           :body
           (json/parse-string true)
           :_source))

  (bulk-get-documents [this index ids]
    (proto/bulk-get-documents this index nil ids))
  (bulk-get-documents [_ index type ids]
    (some-> (http/post (gen-url false (::url config) index type "_mget")
                       {:content-type :json
                        :form-params  {:ids ids}
                        :insecure?    true})
            :body
            (json/parse-string true)
            :docs
            (->> (keep :_source))))

  (documents [this index query]
    (proto/documents this index nil query))
  (documents [_ index type query]
    (let [time-out   (str "_search?scroll=" (or (:time-out query) "5s"))
          base-url   (::url config)
          search-url (gen-url false base-url index type time-out)
          scroll-url (gen-url false base-url "_search" "scroll")
          parse      (comp (partial map :_source) :hits :hits)
          request    (fn [url params]
                       (-> (http/post url  {:content-type :json
                                            :form-params  params
                                            :insecure?    true})
                           :body
                           (json/parse-string true)))

          next-batch (fn next-batch [scroll-id]
                       (lazy-seq
                        (let [body    (if scroll-id
                                        (request scroll-url
                                                 {:scroll_id scroll-id
                                                  :scroll    time-out})
                                        (request search-url
                                                 (cond-> (dissoc query :time-out)
                                                   (not (:sort query)) (assoc :sort ["_doc"]))))
                              records (parse body)]
                          (when (seq records)
                            (concat records (next-batch (:_scroll_id body)))))))]
    (next-batch nil)))

  (search [this index query]
    (proto/search this index nil query))
  (search [_ index type query]
    (-> (http/post (gen-url false (::url config) index type "_search")
                   {:content-type :json
                    :form-params  query
                    :insecure?    true})
        :body
        (json/parse-string true))))

(defn http-elasticsearch
  "Takes a config map with ::url key, returns HTTPElasticsearch record."
  [config]
  {:pre [(s/valid? ::config config)]}
  (->HTTPElasticsearch config))

(s/def ::url string?)
(s/def ::config (s/keys :req [::url]))

(s/fdef http-elasticsearch
        :args (s/cat :config ::config))
