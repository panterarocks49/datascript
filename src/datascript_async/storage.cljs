(ns datascript-async.storage
  (:require
   [datascript-async.db :as db]
   [me.tonsky.maybe-promise :as mp]
   [datascript-async.util :as util]
   [me.tonsky.persistent-sorted-set.storage :as set.storage]
   [me.tonsky.persistent-sorted-set-async :as set]))

(defprotocol IStorage
  :extend-via-metadata true
  (-accessed [_ addr]
    "Called when an address is accessed.")
  (-store [_ addr+node-seq]
    "Gives you a sequence of `[addr data]` pairs to serialize and store.
     `addr`s are either nil if it's an unstored node or
       a past generated addr if it was already stored but was choosen to store again based on store-group-size
     `data`s are a node/leaf of the set
     the exception to this is root and tail addrs
       They have special addrs \"root\" and \"tail\"
       those will be clojure serializable structures
     You need to return a list of addresses from this which correspond to
     the addresses of the passed in nodes (in order)")
  (-restore [_ addr]
    "Read back and deserialize data stored under single `addr`")
  (-list-addresses [_]
    "Return seq that lists all addresses currently stored in your storage.
     Will be used during GC to remove keys that are no longer used.")
  (-delete [_ addrs-seq]
    "Delete data stored under `addrs` (seq). Will be called during GC"))

(def ^:private root-addr
  "root")

(def ^:private tail-addr
  "tail")

(deftype StorageAdapter [storage]
  set.storage/IStorage
  (store [_ addr+nodes]
    (-store storage addr+nodes))
  (accessed [_ addr]
    (-accessed storage addr))
  (restore [_ addr]
    (-restore storage addr)))

(defn make-storage-adapter [storage]
  (StorageAdapter. storage))

(defn maybe-adapt-storage [opts]
  (if (:storage opts)
    (update opts :storage make-storage-adapter)
    opts))

(defn storage-adapter ^StorageAdapter [db]
  (when db
    (.-_storage ^set/BTSet (:eavt db))))

(defn storage [db]
  (when-some [adapter (storage-adapter db)]
    (.-storage adapter)))

(def ^:private stored-dbs
  #js [])

(defn- remember-db [db]
  (.push stored-dbs (js/WeakRef. db)))

(defn store-impl! [db ^StorageAdapter adapter force?]
  (mp/locking (:storage adapter)
    (mp/do
      (remember-db db)
      (let [old-addrs  (mapv
                        (fn [^set/BTSet s]
                          (.-_address s))
                        [(:eavt db) (:aevt db) (:avet db)])
            ;; try to store in parallel
            peavt-addr (set/store (:eavt db) adapter)
            paevt-addr (set/store (:aevt db) adapter)
            pavet-addr (set/store (:avet db) adapter)]
        (mp/let [eavt-addr peavt-addr
                 aevt-addr paevt-addr
                 avet-addr pavet-addr
                 meta      {:schema        (:schema db)
                            :max-eid       (:max-eid db)
                            :max-tx        (:max-tx db)
                            :eavt          eavt-addr
                            :aevt          aevt-addr
                            :avet          avet-addr
                            :eavt-metadata (set/set-metadata (:eavt db))
                            :aevt-metadata (set/set-metadata (:aevt db))
                            :avet-metadata (set/set-metadata (:avet db))
                            :settings      (-> (set/settings (:eavt db))
                                               (dissoc :make-reference :read-reference))}]
          (when (or force? (some nil? old-addrs))
            (-store (.-storage adapter) [[root-addr meta] [tail-addr []]]))
          db)))))

(defn store
  ([db]
   (if-some [adapter (storage-adapter db)]
     (store-impl! db adapter false)
     (throw (ex-info "Database has no associated storage" {}))))
  ([db storage]
   (if-some [adapter (storage-adapter db)]
     (let [current-storage (.-storage adapter)]
       (if (identical? current-storage storage)
         (store-impl! db adapter false)
         (throw (ex-info "Database is already stored with another IStorage" {:storage current-storage}))))
     (let [adapter (StorageAdapter. storage)]
       (store-impl! db adapter false)))))

(defn store-tail [db tail]
  (-store (storage db) [[tail-addr tail]]))

(defn restore-impl [storage opts]
  ;; TODO: do I need async-locking here?
  ;; not sure why it was used in the clojure impl
  (mp/let [root (-restore storage root-addr)]
    (when root
      (mp/let [tail    (-restore storage tail-addr)
               {:keys [schema eavt aevt avet max-eid max-tx settings
                       eavt-metadata aevt-metadata avet-metadata]} root
               opts    (merge settings opts)
               adapter (make-storage-adapter storage)
               eavt    (set/restore-by db/cmp-datoms-eavt eavt adapter (assoc opts :set-metadata eavt-metadata))
               aevt    (set/restore-by db/cmp-datoms-aevt aevt adapter (assoc opts :set-metadata aevt-metadata))
               avet    (set/restore-by db/cmp-datoms-avet avet adapter (assoc opts :set-metadata avet-metadata))
               db      (db/restore-db
                        {:schema  schema
                         :eavt    eavt
                         :aevt    aevt
                         :avet    avet
                         :max-eid max-eid
                         :max-tx  max-tx})]
        ;; TODO: read all branch nodes
        (set/-root eavt)
        (set/-root aevt)
        (set/-root avet)
        (remember-db db)
        [db tail]))))

(defn db-with-tail [db tail]
  (mp/reduce
   (fn [db datoms]
     (if (empty? datoms)
       db
       (as-> db %
         (mp/reduce db/with-datom % datoms)
         (assoc % :max-tx (:tx (first datoms))))))
   db
   tail))

(defn restore
  ([storage]
   (restore storage {}))
  ([storage opts]
   (mp/let [[db tail] (restore-impl storage opts)]
     (db-with-tail db tail))))

(defn- addresses-impl [db visit-fn]
  {:pre [(db/db? db)]}
  (mp/do
    (set/-walk-addresses (:eavt db) visit-fn)
    (set/-walk-addresses (:aevt db) visit-fn)
    (set/-walk-addresses (:avet db) visit-fn)))

(defn addresses [dbs]
  (let [*set     (volatile! (transient #{}))
        visit-fn #(vswap! *set conj! %)]
    (visit-fn root-addr)
    (visit-fn tail-addr)
    (mp/do
      (mp/loop [dbs dbs]
        (when (seq dbs)
          (mp/do
            (addresses-impl (first dbs) visit-fn)
            (mp/recur (rest dbs)))))
      (persistent! @*set))))

;; don't use this, this is for tests
(defn fake-system-gc []
  (set! stored-dbs (.map stored-dbs (fn [_] (js/WeakRef. #js [])))))

(defn- read-stored-dbs [storage']
  ;; remove all empty refs
  (set! stored-dbs (.filter stored-dbs (fn [^js ref] (.deref ref))))
  (mp/loop [dbs (into [] stored-dbs)
            res (transient [])]
    (if (seq dbs)
      (let [ref (first dbs)
            db  (.deref ^js ref)]
        (if (and (db/db? db)
                 (identical? (storage db) storage'))
          (mp/recur (rest dbs) (conj! res db))
          (mp/recur (rest dbs) res)))
      (persistent! res))))

(defn collect-garbage
  ([storage']
   (collect-garbage storage' []))
  ([storage' extra-dbs]
   (mp/locking storage'
     (mp/let [dbs    (read-stored-dbs storage')
              db     (restore storage')
              dbs    (into (conj dbs db) extra-dbs) ;; make sure we won’t gc currently stored db
              used   (addresses dbs)
              all    (-list-addresses storage')
              unused (into [] (remove used) all)]
       (util/log "GC: found" (count dbs) "alive db refs," (count used) "used addrs," (count all) "total addrs," (count unused) "unused")
       (-delete storage' unused)))))

