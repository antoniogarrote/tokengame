(ns tokengame.actors
  (:use [jobim])
  (:use [tokengame.pnml]
        [tokengame.core]
        [tokengame.utils])
  (:use [clojure.contrib.logging :only [log]]))

;; protocol

(defn protocol-commit
  ([from num-tokens]
     {:topic :commit
      :from from
      :num-tokens num-tokens}))

(defn protocol-token-add
  ([from token]
     {:topic :token-add
      :from from
      :token token}))

(defn protocol-commit-ok
  ([from]
     {:topic :commit-ok
      :from from}))

(defn protocol-rollback-from-transition
  ([from]
     {:topic :rollback
      :from from}))

(defn protocol-token-update
  ([id count]
     {:topic :token-update
      :id id
      :num-tokens count}))

(defn protocol-committed
  ([id tokens]
     {:topic :committed
      :tokens tokens
      :id id}))

(defn protocol-rollback-from-place
  ([id count]
     {:topic :rollback
      :count count
      :id id}))

;; Utility functions

(defn transition-name
  ([transition]
     (str "transition-" (:name transition))))

(defn update-state
  ([state new-state]
     (swap! state (fn [_] new-state))))

(defn set-atom
  ([a v]
     (swap! a (fn [_] v))))

;; place actor


(defn append-token
  ([tokens token]
     (swap! tokens (fn [tks] (conj tks token)))))

(defn add-to-front
  ([tokens to]
     (swap! to (fn [old] (concat tokens old)))))

(defn send-all-transitions
  ([msg transitions]
     (doseq [transition transitions]
       (do
         (log :info (str " *** sending to transtion " (transition-name transition) " message " msg))
         (send! (resolve-name (transition-name transition)) msg)))))

(defn move-n-tokens
  ([num from to]
     (let [taken (take num @from)
           fromp (swap! from (fn [old] (drop num old)))]
       (set-atom to taken))))

(defn handle-token-add
  ([place msg state tokens committed transitions]
     (log :info (str "** place " (:id place) " handling token-add with state " @state))
     (condp = @state
       :normal     (do (append-token tokens (:token msg))
                       (send-all-transitions (protocol-token-update (:id place) (count @tokens)) (shuffle transitions)))
       :committing (append-token tokens (:token msg))
       (throw (Exception. "Unknown state")))))

(defn handle-normal-commit
  ([place msg state tokens committed transitions transition-committing]
     (if (> (:num-tokens msg) (count @tokens))
       (send! (:from msg) (protocol-rollback-from-place (:id place) (count @tokens)))
       (do (update-state state :committing)
           (set-atom transition-committing (:from msg))
           (move-n-tokens (:num-tokens msg) tokens committed)
           (send! (:from msg) (protocol-committed (:id place) @committed))))))

(defn handle-committing-commit
  ([place msg state tokens committed transitions transition-committing]
     (send! (:from msg) (protocol-rollback-from-place (:id place) (:count @tokens)))))

(defn handle-commit
  ([place msg state tokens committed transitions transition-committing]
     (condp = @state
       :normal     (handle-normal-commit place msg state tokens committed transitions transition-committing)
       :committing (handle-committing-commit place msg state tokens committed transitions transition-committing)
       (throw (Exception. "Unknown state")))))

(defn handle-commit-ok
  ([place msg state tokens committed transitions transition-committing]
     (condp = @state
       :committing (when (= @transition-committing (:from msg))
                     (do (update-state state :normal)
                         (set-atom committed [])
                         (set-atom transition-committing :none)
                         (send-all-transitions (protocol-token-update (:id place) (count @tokens)) transitions)))
       (throw (Exception. "wrong state handling handle-commit-ok")))))

(defn handle-rollback-place
  ([place msg state tokens committed transitions transition-committing]
     (condp = @state
       :committing (when (= (:from msg) @transition-committing)
                     (do (update-state state :normal)
                         (add-to-front @committed tokens)
                         (set-atom committed [])
                         (set-atom transition-committing :none)
                         (send-all-transitions (protocol-token-update (:id place) (count @tokens)) transitions)))
       :normal      :ignore
       (throw (Exception. "wrong state handling rollback")))))

(defn place-actor
  ([place model]
     (let [state (atom :normal)
           tokens (atom [])
           committed (atom [])
           transitions (out-transitions-for-place place model)
           transition-committing (atom :none)]
       (react-loop []
        (react [msg]
               (do
                 (log :info (str "*** place " (:id place) " got message"))
                 (condp = (:topic msg)
                   :token-add (handle-token-add place msg state tokens committed transitions)
                   :commit    (handle-commit place msg state tokens committed transitions transition-committing)
                   :commit-ok (handle-commit-ok place msg state tokens committed transitions transition-committing)
                   :rollback  (handle-rollback-place place msg state tokens committed transitions transition-committing)
                   :state (send! (:from msg) {:tokens @tokens :committed @committed :state @state :transition-committing  @transition-committing})
                   (throw (Exception. "unknown topic place" (:topic msg))))
                 (react-recur)))))))

;; transition actor

(defn create-to-commit
  ([places-in-state to-commit]
     (let [to-commit-p (reduce (fn [m p] (assoc m p :none)) {} (keys @places-in-state))]
       (set-atom to-commit to-commit-p))))

(defn fire-ready?
  ([places-in-state transition]
     (loop [places-in (keys @places-in-state)]
       (if (empty? places-in)
         true
         (let [place-in (first places-in)
               min-count (:size (:place (get @places-in-state place-in)))
               count  (:count (get @places-in-state place-in))]
           (if (or (= count :unknown)
                   (< count min-count))
             false
             (recur (rest places-in))))))))

(defn place-name
  ([place] (str "place-" (:id place))))

(defn send-all-places-in
  ([places-in-state f]
     (doseq [place-in (keys @places-in-state)]
       (do
         (log :info (str " *** sending to place " (place-name (:place (get @places-in-state place-in))) " message " (f (get @places-in-state place-in))))
         (send! (resolve-name (place-name (:place (get @places-in-state place-in)))) (f (get @places-in-state place-in)))))))

(defn update-places-in-state
  ([places-in-state place-id num-tokens]
     (swap! places-in-state (fn [m] (let [v (get m place-id)]
                                      (assoc m place-id
                                             (assoc v :count num-tokens)))))))

(defn reset-places-in-state
  ([places-in-state]
     (swap! places-in-state
            (fn [m]
              (reduce (fn [mp k]
                        (let [old-state (get m k)
                              new-state (assoc old-state :count :unknown)]
                          (assoc m k new-state)))
                      {}
                      (keys m))))))

(defn handle-normal-token-update
  ([transition msg out-places places-in-state to-commit state]
     (update-places-in-state places-in-state (:id msg) (:num-tokens msg))
     (when (fire-ready? places-in-state transition)
       (do (create-to-commit places-in-state to-commit)
           (update-state state :commit-init)
           (send-all-places-in places-in-state #(protocol-commit (self) (:count %1)))
           (reset-places-in-state places-in-state)))))

(defn handle-commit-init-token-update
  ([transition msg out-places places-in-state to-commit state]
     (update-places-in-state places-in-state (:id msg) (:num-tokens msg))))

(defn handle-token-update
  ([transition msg out-places places-in-state to-commit state]
     (condp = @state
       :normal      (handle-normal-token-update transition msg out-places places-in-state to-commit state)
       :commit-init (handle-commit-init-token-update transition msg out-places places-in-state to-commit state)
       (throw (Exception. "unknown state handling handle-token-update")))))


(defn handle-rollback-transition-commit-init
  ([transition msg out-places places-in-state to-commit state]
     (send-all-places-in places-in-state (fn [_] (protocol-rollback-from-transition (self))))
     (reset-places-in-state places-in-state)
     (update-state state :normal)))

(defn handle-rollback-transition-normal
  ([transition msg out-places places-in-state to-commit state]
     (update-places-in-state places-in-state (:id msg) (:num-tokens msg))))

(defn handle-rollback-transition
  ([transition msg out-places places-in-state to-commit state]
     (condp = @state
       :commit-init (handle-rollback-transition-commit-init
                     transition msg out-places places-in-state to-commit state)
       :normal      (handle-rollback-transition-normal
                     transition msg out-places places-in-state to-commit state)
       (throw (Exception. "unknown state handling handle-rollback-transition")))))


(defn all-committed?
  ([to-commit]
     (not (some #(= %1 :none) (vals @to-commit)))))

(defn update-to-commit
  ([to-commit msg]
     (let [_ (log :info (str " *** before update to-commit " @to-commit))
           _ (log :info (str " *** updating " msg))
           id (:id msg)
           tokens (:tokens msg)]
       (swap! to-commit (fn [m] (assoc m id tokens)))
       (log :info (str " *** after update to-commit " @to-commit)))))

(defn apply-transition-fn
  ([f to-commit]
     (let [tokens (reduce concat [] (vals @to-commit))]
       (log :info (str "*** about to apply " f " to tokens " tokens))
       (apply f tokens))))

(defn send-all-places-out
  ([places-out msg]
     (doseq [place places-out]
       (send! (resolve-name (place-name place)) msg))))

(defn handle-committed
  ([transition msg out-places places-in-state to-commit state f]
     (condp = @state
       :commit-init (do (update-to-commit to-commit msg)
                        (when (all-committed? to-commit)
                          (let [_ (log :info (str " *** applying function " f " to tokens " @to-commit))
                                token-out (apply-transition-fn f to-commit)]
                            (reset-places-in-state places-in-state)
                            (send-all-places-in places-in-state (fn [_] (protocol-commit-ok (self))))
                            (send-all-places-out out-places (protocol-token-add (self) token-out))
                            (update-state state :normal))))
       :normal      :ignore
       (throw (Exception. "error state handling handle-committed")))))

(defn transition-actor
  ([transition model f]
     (let [out-places (:places-out transition)
           places-in-state (atom (reduce (fn [m p] (assoc m (:id p) {:count :unknown :place p})) {} (:places-in transition)))
           to-commit (atom {})
           state (atom :normal)]
       (react-loop []
        (react [msg]
               (do
                 (log :info (str "*** transition " (transition-name transition) " got message " msg))
                 (condp = (:topic msg)
                   :token-update (handle-token-update transition msg out-places places-in-state to-commit state)
                   :rollback     (handle-rollback-transition transition msg out-places places-in-state to-commit state)
                   :committed    (handle-committed transition msg out-places places-in-state to-commit state f)
                   :state        (send! (:from msg) {:state @state :places-in @places-in-state :to-commit @to-commit})
                   (throw (Exception. "unknown topic actor" (:topic msg))))
                 (react-recur)))))))


;; Rewritten core.clj

(defmulti run
  "Executes one component of the model"
  (fn [component model] (:model-component component)))

(defmethod run :place
  ([component model]
     (log :info (str "** running place: " component))
     (let [pid (spawn-evented #(place-actor component model))]
       (register-name (place-name component) pid)
       (log :info (str "** registered place " (place-name component) " with PID " pid)))))

(defmethod run :transition
  ([component model]
     (log :info (str "** running transition: " component))
     (let [f (eval-ns-fn (:function component))
           _ (log :info (str "** passing function " f " to transition parsed after " (:function component)))
           pid (spawn-evented #(transition-actor component model f))]
       (register-name (transition-name component) pid)
       (log :info (str "** registered transition " (transition-name component) " with PID " pid)))))


(defn petri-controller-actor
  ([name model]
     (let [pid (spawn #(loop [msg (receive)]
                         (do
                           (condp = (:cmd msg)
                             :run-component (do (log :info (str "** controller " name " starting component " (:component msg)))
                                                (run (:component msg) model))
                             (log :error (str "** controller " name " unknown message received " msg)))
                           (recur (receive)))))]
       (register-name name pid)
       (str "** controller " name " initialized with PID " pid))))

(defn ensure-pid
  ([]
     (try (self) (catch Exception ex (spawn-in-repl)))))

(defn controller-name
  ([node]
     (str "petri-controller-" node)))

(defn run-component-msg
  ([from component]
     {:from from :component component :cmd :run-component}))

(defn run-petri-net
  ([pnml-file nodes-file]
     (let [model (parse-pnml (java.io.File. pnml-file))
           nodes (eval (read-string (slurp nodes-file)))]
       ;; debug
       (org.apache.log4j.PropertyConfigurator/configureAndWatch "/Users/antonio.garrote/Development/old/tokengame/lib/log4j.properties" 1000)
       ;; end debug
       (ensure-pid)
       ;; start nodes
       (doseq [node (keys nodes)]
         (do
           (rpc-blocking-call (resolve-node-name node) "tokengame.actors/petri-controller-actor" [(controller-name node) model])))
       ;; start components
       (doseq [node (keys nodes)]
         (do
           ;; run places
           (doseq [place  (:places (get nodes node))]
             (send! (resolve-name (controller-name node)) (run-component-msg (self) (find-place model place))))
           ;; run transitions
           (doseq [transition  (:transitions (get nodes node))]
             (send! (resolve-name (controller-name node)) (run-component-msg (self) (find-transition model transition))))))
       :ok)))

;; Utility functions

(defn fire
  ([place token]
     (ensure-pid)
     (log :info (str "adding token " token " to place " (place-name place)))
     (send! (resolve-name (place-name place)) (protocol-token-add (self) token))))

(defn place-state
  ([place]
     (ensure-pid)
     (log :info (str "retrieving state for place " (place-name place)))
     (send! (resolve-name (place-name place)) {:from (self) :topic :state})
     (receive)))

(defn transition-state
  ([transition]
     (ensure-pid)
     (log :info (str "retrieving state for transition " (transition-name transition)))
     (send! (resolve-name (transition-name transition)) {:from (self) :topic :state})
     (receive)))
