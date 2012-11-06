(ns rrp.rrp
  "Reactive queries in datomic"
  (:use [datomic.api :only [q db] :as d])
  (:use clojure.repl)
  (:use clojure.pprint))

(comment
  For every datum of the form
  (entity relation value)
  And every query made of clauses of the form
  (subject predicate object)
  We can check if a query is related to an datum,
  if one clause's predicate matches the datums relation the query is related to the datum.
  Except if the clauses subject and object are parameterized, but we handle that too, by checking if the values of parameterized variables,
  match with the datum.
  For a new datum the query is executed with the clause subject and object replaced by the datums entity and value

  Query

  [?p :person/friend ?f]
  [?f :person/message ?m]

  Datum

  [:user1 :person/friend :user2]

  Reactive Query

  [:user1 :person/friend :user2]
  [:user2 :person/message ?m]

  To try it out just run the function push in the repl and hopefully see users->queries get updated accordingly.

  Obviously this will only work for a subset of datomics queries.)

(def uri "datomic:mem://rrp19")

(d/create-database uri)

(def conn (d/connect uri))

(defn load-query-schema []
  "A query consists of the input variables (saved not exactly elegantly as
   strings) the output variables (dito) and multiple
   clauses (no support for data sources or rules yet)"
  (let [schema [{:db/id #db/id[:db.part/db]
                 :db/ident :query/in
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/many
                 :db/doc "A querys inputs"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :query/clause
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/many
                 :db/doc "A querys clause"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :query/out
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/many
                 :db/doc "A querys clause"
                 :db.install/_attribute :db.part/db}
                ;;A Clause is of the typical subject prediate object form
                {:db/id #db/id[:db.part/db]
                 :db/ident :clause/subject
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db/doc "A guild's realm"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :clause/object
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db/doc "A clause object"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :clause/predicate
                 :db/valueType :db.type/keyword
                 :db/cardinality :db.cardinality/one
                 :db/doc "A clauses predicate"
                 :db.install/_attribute :db.part/db}
                ;;Just some simple example relationship
                ]]
    @(d/transact conn schema)))

(defn load-social-schema []
  "Just some generic social schema.
   Each person may have multiple friends.
   Each person may have multiple messages.
"
  (let [schema [{:db/id #db/id[:db.part/db]
                 :db/ident :person/name
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db/fulltext true
                 :db/doc "A guilds name"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :person/friend
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/many
                 :db/doc "A persons friend"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :message/person
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/one
                 :db/doc "Poster of a message"
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id[:db.part/db]
                 :db/ident :message/text
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db/doc "Text of a message"
                 :db.install/_attribute :db.part/db}]]
    @(d/transact conn schema)))

(defn clause [[subject predicate object]]
  (let [cid  (d/tempid :db.part/user)]
    [cid {:db/id cid
          :clause/subject subject
          :clause/object object
          :clause/predicate predicate}]))


(defn save-query
  ([output input clauses id]
     "Store a query, which finds all friends of a user in the database."
     (let [clauses (map clause clauses)
           guild-seed (conj (map second clauses)
                            {:db/id id
                             :query/in input
                             :query/out output
                             :query/clause (map first clauses)})]
       @(d/transact conn guild-seed)))
  ([output input clauses]
     (save-query output input clauses (d/tempid :db.part/user))))

(defn load-message-query []
  "Store a query for finding all messages of a user in the database."
  (save-query ["?u"] ["?m"]
              ["?m" :message/person "?p"] #db/id[:db.part/user 2]))

(defn load-friends-query []
  "Store a query, which finds all friends of a user in the database."
  (save-query ["?f"] ["?u"] [["?u" :person/friend "?f"]] #db/id[:db.part/user 7]))

(defn load-friends-message-query []
  "Store a query for finding all messages of a user in the database."
  (save-query ["?u" "?f"] ["?m"] [["?u" :person/friend "?f"]
                                  ["?m" :message/person "?f"]] #db/id[:db.part/user 13]))

(defn load-db []
  "Load schemas and queries"
  (do (load-query-schema)
      (load-social-schema)
      (load-message-query)
      (load-friends-query)
      (load-friends-message-query)))

(def queue
  (d/tx-report-queue conn))

(defn push []
  "Adding some examplary data"
  (let [tid (d/tempid :db.part/user)
        pid (d/tempid :db.part/user)]
    @(d/transact conn [{:db/id pid
                        :person/name "Random"
                        :person/friend []}
                       {:db/id #db/id [:db.part/user 3]
                        :person/name "Hannes"
                        :person/friend [pid]}
                       {:db/id tid
                        :message/person pid
                        :message/text "Blahblah"}])))

(defn get-queries [dbval]
  "Get all queries in the datababase"
  (q '[:find ?q
       :where
       [?q :query/in ?i]
       [?q :query/out ?o]
       ;; [?q :query/clause ?c]
       ] dbval))

(defn build-clause
  [{s :clause/subject p :clause/predicate o :clause/object}]
  "Building a clause from a map."
  [(symbol s) p (symbol o)])

(defn build-query [{in :query/in out :query/out cs :query/clause}]
  [(mapv symbol out)
   (mapv symbol in)
   (mapv build-clause cs)])

;;TODO one user may run the same query with different args
(def queries->queried
  (ref {}))

(defn finish-query [[o i c]]
  (vec (concat [:find]
               o
               '[:in   $]
               i
               [:where ]
               c)))

(def users->queries
  (ref {}))

;;Something like subscribe
(defn run-query [dbval query user args ]
  (let [results
        (apply q (finish-query (build-query (d/entity dbval query))) dbval (vals args))]
    (dosync
     (alter queries->queried #(assoc-in % [query user] args))
     (alter users->queries #(assoc-in % [user query] results)))))

(defn queries-related-to-datums [dbval datums]
  (q '[:find ?q ?s ?o ?e ?aname ?v ;;?added
       :in $ [[?e ?a ?v _ ?added]]
       :where
       [?e ?a ?v _ ?added]
       [?a :db/ident ?aname]
       [?q :query/clause ?c]
       [?c :clause/predicate ?aname]
       [?c :clause/subject ?s]
       [?c :clause/object ?o]
       ] dbval datums))

(defn overwrite-args [q bindings]
  (assoc q 1 (map symbol (keys bindings))))

(defn propagate []
  "Get's all related queries.
   If subject and object are free variables run the query as mentioned above.
   If they are parameterized check if the arguments match with datum, and if they do
   run the query as mentioned above."
  (let [report (.take queue)
        time   (:db-after report)
        datums (:tx-data report)
        queries (queries-related-to-datums time datums)]
    (doall (for [[query-id subject object entity aname value] queries
                 [user-id v] (@queries->queried query-id)]
             ;;TODO query is built every loop iteration now :(
             (let [query (build-query (d/entity time query-id))
                   specialized-args
                   (cond (and (not (contains? v subject)) (not (contains? v object)))
                         (assoc v subject entity object value)
                         (and (not (contains? v subject)) (= (v object) value))
                         (assoc v  subject entity)
                         (and (not (contains? v object)) (= (v subject) entity))
                         (assoc v  object value))
                   result (if specialized-args
                            ;;specialized-args
                            (apply q (finish-query (overwrite-args query
                                                                   specialized-args)) time (vals specialized-args)))]
               (dosync (alter users->queries #(update-in % [user-id query-id] concat result))))))))

(def propagator
  (Thread.
   (fn []
     (while true
       (propagate)))))

;;TODO: potential bug: if propagate runs after push and runquery
;;things get pushed to the user twice
;;Solution call propagate from push?
(load-db)

(.start propagator)

(run-query (db conn) (:db/id (d/entity (db conn) #db/id[:db.part/user 7]))
           (:db/id (d/entity (db conn) #db/id[:db.part/user 3]))
           {"?u" (:db/id (d/entity (db conn) #db/id[:db.part/user 3]))})

