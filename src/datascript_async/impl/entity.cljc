(ns ^:no-doc datascript-async.impl.entity
  (:refer-clojure :exclude [keys get])
  (:require
   [#?(:cljs cljs.core :clj clojure.core) :as c]
   [promesa.core :as p]
   [me.tonsky.maybe-promise :as mp]
   [datascript-async.util :as util]
   [datascript-async.db :as db :refer [Datom]]))

(declare entity Entity equiv-entity lookup-entity touch hash-entity)

(defn- entid [db eid]
  (when (or (number? eid)
            (sequential? eid)
            (keyword? eid))
    (db/entid db eid)))

(defn entity [db eid]
  {:pre [(db/db? db)]}
  (mp/let [e (entid db eid)]
    (when e
      ;; TODO: this is slower than seek, maybe impl seek?
      (mp/let [edatoms (db/-datoms db :eavt eid nil nil nil)]
        (when-some [^Datom fdatom (first edatoms)]
          (when (== e (.-e fdatom))
            (Entity. db e edatoms (volatile! false) (volatile! {}))))))))

(defn- entity-attr [db a datoms]
  (if (db/multival? db a)
    (if (db/ref? db a)
      (mp/let [acc (mp/reduce
                    (fn [acc ^Datom datom]
                      (mp/let [ent (entity db (.-v datom))]
                        (conj! acc ent)))
                    (transient #{})
                    datoms)]
        (persistent! acc))
      (persistent!
       (reduce
        (fn [acc ^Datom datom]
          (conj! acc (.-v datom)))
        (transient #{})
        datoms)))
    (if (db/ref? db a)
      (entity db (.-v ^Datom (first datoms)))
      (.-v ^Datom (first datoms)))))

(defn- -lookup-backwards [db eid attr not-found]
  (mp/let [datoms (db/-search db [nil attr eid])]
    (if-let [datoms (not-empty datoms)]
      (if (db/component? db attr)
        (entity db (.-e ^Datom (first datoms)))
        (mp/let [acc
                 (mp/reduce
                  (fn [acc ^Datom datom]
                    (conj! acc (entity db (.-e datom))))
                  (transient #{})
                  datoms)]
          (persistent! acc)))
      not-found)))

#?(:cljs
   (defn- multival->js [val]
     (when val (to-array val))))

#?(:cljs
   (defn- js-seq [e]
     (touch e)
     (for [[a v] @(.-cache e)]
       (if (db/multival? (.-db e) a)
         [a (multival->js v)]
         [a v]))))

#?(:cljs
  (unchecked-set (.-prototype ES6Iterator) cljs.core/ITER_SYMBOL
     (fn []
       (this-as this# this#))))

#?(:cljs
   (unchecked-set (.-prototype ES6EntriesIterator) cljs.core/ITER_SYMBOL
                  (fn []
                    (this-as this# this#))))

#?(:cljs
   (deftype Entity [db eid edatoms touched cache]
     ;; TODO: js api broken
     Object
     (toString [this]
       (pr-str* this))
     (equiv [this other]
       (equiv-entity this other))

     ;; js/map interface
     (keys [this]
       (es6-iterator (c/keys this)))
     (entries [this]
       (es6-entries-iterator (js-seq this)))
     (values [this]
       (es6-iterator (map second (js-seq this))))
     (has [this attr]
       (p/let [v (.get this attr)]
         (not (nil? v))))
     (get [this attr]
       (p/do!
        (if (= attr ":db/id")
          eid
          (if (db/reverse-ref? attr)
            (mp/let [v (-lookup-backwards db eid (db/reverse-ref attr) nil)]
              (multival->js v))
            (mp/let [v (lookup-entity this attr)]
              (cond-> v
                (db/multival? db attr) multival->js))))))
     (forEach [this f]
       (doseq [[a v] (js-seq this)]
         (f v a this)))
     (forEach [this f use-as-this]
       (doseq [[a v] (js-seq this)]
         (.call f use-as-this v a this)))

     ;; js fallbacks
     (key_set   [this] (to-array (c/keys this)))
     (entry_set [this] (to-array (map to-array (js-seq this))))
     (value_set [this] (to-array (map second (js-seq this))))

     IEquiv
     (-equiv [this o] (equiv-entity this o))

     IHash
     (-hash [this]
       (hash-entity this))

     ISeqable
     (-seq [this]
       (touch this)
       (seq @cache))

     ICounted
     (-count [this]
       (touch this)
       (count @cache))

     ILookup
     (-lookup [this attr]           (lookup-entity this attr nil))
     (-lookup [this attr not-found] (lookup-entity this attr not-found))

     IAssociative
     (-contains-key? [this k]
       (not= ::nf (lookup-entity this k ::nf)))

     IFn
     (-invoke [this k]
       (lookup-entity this k))
     (-invoke [this k not-found]
       (lookup-entity this k not-found))

     IPrintWithWriter
     (-pr-writer [_ writer opts]
       (-pr-writer (assoc @cache :db/id eid) writer opts))))
#?(:clj
   (deftype Entity [db eid edatoms touched cache]
     Object
     (toString [e]      (pr-str (assoc @cache :db/id eid)))
     (hashCode [e]      (hash-entity e))
     (equals [e o]      (equiv-entity e o))

     clojure.lang.Seqable
     (seq [e]           (touch e) (seq @cache))

     clojure.lang.Associative
     (equiv [e o]       (equiv-entity e o))
     (containsKey [e k] (not= ::nf (lookup-entity e k ::nf)))
     (entryAt [e k]     (some->> (lookup-entity e k) (clojure.lang.MapEntry. k)))

     (empty [e]         (throw (UnsupportedOperationException.)))
     (assoc [e k v]     (throw (UnsupportedOperationException.)))
     (cons  [e [k v]]   (throw (UnsupportedOperationException.)))
     (count [e]         (touch e) (count @(.-cache e)))

     clojure.lang.ILookup
     (valAt [e k]       (lookup-entity e k))
     (valAt [e k not-found] (lookup-entity e k not-found))

     clojure.lang.IFn
     (invoke [e k]      (lookup-entity e k))
     (invoke [e k not-found] (lookup-entity e k not-found))))

(defn entity? [x] (instance? Entity x))

#?(:cljs
   (unchecked-set (.-prototype Entity) cljs.core/ITER_SYMBOL
                  (fn []
                    (this-as this# (.entries this#)))))

#?(:clj
   (defmethod print-method Entity [e, ^java.io.Writer w]
     (.write w (str e))))

(defn- equiv-entity [^Entity this that]
  (and
    (instance? Entity that)
    (identical? (.-db this) (.-db ^Entity that)) ; `=` and `hash` on db is expensive
    (= (.-eid this) (.-eid ^Entity that))))

(defn- hash-entity [^Entity e]
  (db/combine-hashes
   (hash (.-eid e))
   ;; A hash compatible with `identical?`. Consistent with `=`.
   (#?(:clj System/identityHashCode :cljs goog/getUid) (.-db e))))

(defn filter-edatoms
  [edatoms a]
  (filter
   (fn [^Datom datom]
     #?(:cljs (keyword-identical? a (.-a datom))
        :clj (identical? a (.-a datom))))
   edatoms))

(defn- lookup-entity
  ([this attr] (lookup-entity this attr nil))
  ([^Entity this attr not-found]
   (if (= attr :db/id)
     (.-eid this)
     (if (db/reverse-ref? attr)
       (-lookup-backwards (.-db this) (.-eid this) (db/reverse-ref attr) not-found)
       (let [cache (.-cache this)]
         (if-some [v (@cache attr)]
           v
           (if @(.-touched this)
             not-found
             (if-some [datoms (not-empty (filter-edatoms (.-edatoms this) attr))]
               (mp/let [value (entity-attr (.-db this) attr datoms)]
                 (vreset! cache (assoc @cache attr value))
                 value)
               not-found))))))))

(defn touch-components [db a->v]
  (mp/let [acc
           (mp/reduce-kv
            (fn [acc a v]
              (mp/let [v (if (db/component? db a)
                           (if (db/multival? db a)
                             (-> (mapv touch v)
                                 (mp/all)
                                 (mp/then set))
                             (touch v))
                           v)]
                (assoc! acc a v)))
            (transient {})
            a->v)]
    (persistent! acc)))

(defn- datoms->cache [db datoms]
  (mp/let [acc
           (mp/reduce
            (fn [acc part]
              (let [a (.-a ^Datom (first part))]
                (mp/let [v (entity-attr db a part)]
                  (assoc! acc a v))))
            (transient {})
            (partition-by #(.-a ^Datom %) datoms))]
    (persistent! acc)))

(defn touch [^Entity e]
  ;; touch is not recommended for production
  ;; datoms are already cached so this is slow
  {:pre [(or (nil? e) (entity? e))]}
  (when (some? e)
    (when-not @(.-touched e)
      (when-let [datoms (not-empty (.-edatoms e))]
        (mp/let [new-cache (datoms->cache (.-db e) datoms)
                 new-cache (touch-components (.-db e) new-cache)]
          (vreset! (.-cache e) new-cache)
          (vreset! (.-touched e) true))))
    e))

#?(:cljs (goog/exportSymbol "datascript-async.impl.entity.Entity" Entity))
