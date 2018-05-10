(ns org.purefn.helmet
  (:refer-clojure :exclude (alias))
  (:require [clojure.spec.alpha :as s]
            [org.purefn.helmet.protocol :as proto]))



(defn create-index
  "Create elastic index, with name and options map with possible keys below:
    * `:mappings`  map with elastic mappings
    * `:settings`  map of settings, custom analyzers, etc.
    * `:shards`    number of shards
    * `:replicas`  number of replicas"
  [es index opts]
  (proto/create-index es index opts))

(defn index-info
  "Returns map of information pertaining to an index or nil if index
  does not exist."
  [es index]
  (proto/index-info es index))

(defn delete-index
  "Delete elastic index."
  [es index]
  (proto/delete-index es index))

(defn alias-indices
  "Create or update alias to point to collection of indices. If
   alias exists, it will be removed from any index not present in
   indices."
  [es alias indices]
  (proto/alias-indices es alias indices))

(defn get-indices
  "Returns all indices referenced by alias."
  [es alias]
  (proto/get-indices es alias))

(defn add-document
  "Adds document of type with specified id to index."
  ([es index id document]
   (proto/add-document es index nil id document))
  ([es index type id document]
   (proto/add-document es index type id document)))

(defn bulk-add-documents
  "Adds a collection of documents of given type to index. Documents
   is either a map of id to document, or a collection of [id document]
   tuples."
  [es index type documents]
  (proto/bulk-add-documents es index type documents))

(defn update-document
  "Updates the document with the given id and type in `index`.
   Delta is a map containing the keys that will be changed/added."
  [es index type id delta]
  (proto/update-document es index type id delta))

(defn delete-document
  "Deletes document with the given id and type in `index`"
  [es index type id]
  (proto/delete-document es index type id))

(defn get-document
  "Retrieves a document - optionally of given type - from
   elasticsearch by id.

   *** To retrieve from all types, use `_all` type ***"
  ([es index type id]
   (proto/get-document es index type id))
  ([es index id]
   (proto/get-document es index id)))

(defn bulk-get-documents
  "Retrieves a collection of documents - optionally of given type -
   from elasticsearch by ids."
  ([es index type ids]
   (proto/bulk-get-documents es index type ids))
  ([es index ids]
   (proto/bulk-get-documents es index ids)))

(defn documents
  "Returns a lazy sequence of documents - optionally of given type -
   from elasticsearch filtered by query.
   Query has optional argument `time-out` used for expiration of scroll id.
   i.e. `1s` `1m`
   Default `time-out` is `5s`"
  ([es index type query]
   (proto/documents es index type query))
  ([es index query]
   (proto/documents es index query)))

(defn search
  "Performs elastic query against index. Returns results converted to
   Clojure map."
  [es index query]
  (proto/search es index query))



;;-------------------------------------------------------
;; specs
;;-------------------------------------------------------

(def helmet? (partial satisfies? proto/Elasticsearch))

(s/def ::mappings map?)
(s/def ::settings map?)
(s/def ::shards pos-int?)
(s/def ::replicas pos-int?)

(s/def ::index-name string?)
(s/def ::alias-name string?)
(s/def ::indices (s/coll-of ::index-name))
(s/def ::aliases (s/coll-of ::alias-name))

(s/def ::id (s/or :string string? :number nat-int?))
(s/def ::ids (s/coll-of ::id))
(s/def ::doctype string?)
(s/def ::document map?)
(s/def ::bulk-document (s/cat :id ::id :doc ::document))

(s/def ::query map?)

(s/def ::index-opts (s/keys :opt-un [::mappings ::settings ::shards ::replicas]))
(s/def ::info (s/keys :opt-un [::aliases ::mappings ::settings]))
(s/def ::index-info (s/or ::info (s/map-of keyword? ::info)))

(s/fdef create-index
        :args (s/cat :es helmet?
                     :index ::index-name
                     :opts ::index-opts)
        :ret boolean?)

(s/fdef index-info
        :args (s/cat :es helmet?
                     :index ::index-name)
        :ret ::index-info)

(s/fdef delete-index
        :args (s/cat :es helmet?
                     :index ::index-name)
        :ret boolean?)

(s/fdef alias-indices
        :args (s/cat :es helmet?
                     :alias ::alias-name
                     :indices ::indices)
        :ret boolean?)

(s/fdef get-indices
        :args (s/cat :es helmet?
                     :alias ::alias-name)
        :ret ::aliases)

(s/fdef add-document
        :args (s/cat :es helmet? :index ::index-name :type ::doctype :id ::id :doc ::document)
        :ret boolean?)

(s/fdef bulk-add-documents
        :args (s/cat :es helmet? :index ::index-name :type ::doctype :docs (s/coll-of ::bulk-document))
        :ret (s/map-of (s/or :num number? :str string?)
                       (s/or :ok true? :error map?)))

(s/fdef delete-document
        :args (s/cat :es helmet? :index ::index-name :type ::doctype :id ::id)
        :ret boolean?)

(s/fdef get-document
        :args (s/cat :es helmet? :index ::index-name :type (s/? ::doctype) :id ::id)
        :ret ::document)

(s/fdef bulk-get-documents
        :args (s/cat :es helmet? :index ::index-name :type (s/? ::docutype) :ids (s/coll-of ::id))
        :ret (s/coll-of ::document))

(s/fdef documents
        :args (s/cat :es helmet? :index ::index-name :type (s/? ::docutype) :query ::query)
        :ret (s/coll-of :document))

(s/fdef search
        :args (s/cat :es helmet? :index ::index-name :query ::query))
