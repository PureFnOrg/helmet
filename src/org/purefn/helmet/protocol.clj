(ns org.purefn.helmet.protocol
  (:refer-clojure :exclude (alias)))

(defprotocol Elasticsearch
  (create-index [this index opts])
  (index-info [this index])
  (delete-index [this index])
  (alias-indices [this alias indices])
  (get-indices [this alias])
  (add-document [this index id document] [this index type id document] )
  (bulk-add-documents [this index documents] [this index type documents])
  (update-document [this index id delta] [this index type id delta])
  (delete-document [this index id] [this index type id])
  (get-document [this index type id] [this index id])
  (bulk-get-documents [this index ids] [this index type ids])
  (documents [this index query] [this index type query])
  (search [this index query] [this index type query]))
