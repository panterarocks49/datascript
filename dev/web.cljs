(ns web
  (:require
   ["idb" :as idb]
   [datascript.core :as d]
   [datascript.db :as ddb]
   [datascript.storage :as storage]
   [promesa.core :as p]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [me.tonsky.persistent-sorted-set :as pss]))

(deftype IDBStorage [*storage]
  storage/IStorage
  (-restore [_ addr]
    (prn "RESTORE" addr)
    (get @*storage addr))

  (-store [_ addr+data-seq]
    (prn "STORE" (mapv first addr+data-seq))
    (swap! *storage into addr+data-seq)
    nil)
  (-list-addresses [_])
  (-delete [_ addrs-seq]))

(defn idb-storage []
  (IDBStorage. (atom {})))

(def cmp compare #_(compare %2 %1))
(def storage (idb-storage))

(def schema {:children {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}})

(def datoms
  (vec
   (mapcat
    (fn [id]
      [(ddb/datom id :block/string "test this")
       (ddb/datom id :children (dec id))
       (ddb/datom (dec id) :block/open true)])
    (range 2 1000))))

(defn print-async
  [f]
  (-> (f)
      (p/catch js/console.error)
      (p/then prn)))


(comment
  (def db (d/init-db datoms schema {}))

  (print-async #(d/store db storage))

  (p/let [d (d/restore storage)]
    (def restored-db d))

  (print-async #(ddb/-datoms restored-db :eavt 2 nil nil nil))

  (print-async #(ddb/-search db [2 :block/string nil nil]))


  (def s (pss/from-sequential
          cmp
          (range 0 10000)
          {:storage storage}))

  (-> (pss/slice s 1000 1700)
      (p/then (fn [v]
                (prn v))))

  )






;; start is called by init and after code reloading finishes
(defn ^:dev/after-load start []
  (js/console.log "start"))

(defn init []
  ;; init is called ONCE when the page loads
  ;; this is called in the index.html and must be exported
  ;; so it is available even in :advanced release builds
  (js/console.log "init")
  (start))

;; this is called before any code is reloaded
(defn ^:dev/before-load stop []
  (js/console.log "stop"))


