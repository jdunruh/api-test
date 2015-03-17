(ns api-test.service
  (:require [pedestal.swagger.core :as swagger]
            [io.pedestal.http :as bootstrap]
            [io.pedestal.http.route :as route]
            [io.pedestal.http.body-params :as body-params]
            [io.pedestal.http.route.definition :refer [defroutes expand-routes]]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.impl.interceptor :refer [terminate]]
            [ring.util.response :refer [response not-found created]]
            [ring.util.codec :as codec]
            [clj-time.core :as t]
            [schema.core :as s]))

;;;; database
(def job-store (atom {}))


;;;; Auxiliary Functions

(defn add-job-to-db [job]
  (swap! job-store assoc-in [:jobs (:id job)] (assoc job :time (t/now))))

(defn job-timed-out? [job-time-stamp end-time]
  (t/before? job-time-stamp end-time))

(defn job-filter [end-time m k v]
  (if (not (job-timed-out? (:time v) end-time))
    (assoc m k v)
    m))

(defn delete-old-jobs [jobs]
  (let [time-filter (partial job-filter (t/minus (t/now) (t/minutes 5)))
        job-map (:jobs jobs)]
    (hash-map :jobs (reduce-kv time-filter {} job-map))))

;;;; Schemas

(def req s/required-key)



(s/defschema Job
  {(req :id) s/Int
   (req :total) s/Int
   (req :progress) s/Int})

(s/defschema jobList
  {(req :total) s/Int
   (req :jobs) [Job]})
;


;

;; Background jobs


(swagger/defhandler get-all-jobs
                    {:summary "Get all jobs in the store"
                     :responses {200 {:schema jobList}}}
                    [_]
                    (response (let [jobs (:jobs (swap! job-store delete-old-jobs))]
                                {:total (count jobs)
                                 :jobs (or (vec (map #(dissoc % :time) (vals jobs))) [])})))

(swagger/defhandler add-job
                    {:summary "Add a new job to the store"
                     :parameters {:body Job}
                     :responses {201 {}}}
                    [{:keys [body-params] :as req}]
                    (let [store (add-job-to-db body-params)]
                      (created (route/url-for ::get-job-by-id :params {:id (:id body-params)}) "")))

(swagger/defbefore load-job-from-db
                   {:description "Assumes a job exists with given ID"
                    :parameters  {:path {:id s/Int}}
                    :responses   {404 {}}}
                   [{:keys [request response] :as context}]
                   (if-let [job (get-in (swap! job-store delete-old-jobs) [:jobs (-> request :path-params :id)])]
                       (assoc-in context [:request ::job] job)
                       (-> context
                         terminate
                         (assoc-in [:response] (not-found "job not found")))))

(swagger/defhandler get-job-by-id
                   {:summary     "Find job by ID"
                    :description "Returns a job based on ID"
                    :responses   {200 {:schema Job}}}
                   [{:keys [::job] :as req}]
                   (response (dissoc job :time)))

(swagger/defhandler update-job
                    {:summary "Update an existing job"
                     :parameters {:body Job}}
                    [{:keys [path-params body-params] :as req}]
                    (let [store (swap! job-store assoc-in [:jobs (:id path-params)] (assoc body-params :time t/now))]
                      (response "OK")))


;

(swagger/defbefore basic-auth
                   {:description "Check basic auth credentials"
                    :security {"basic" []}
                    :responses {403 {}}}
                   [{:keys [request response] :as context}]
                   (let [auth (get-in request [:headers :authorization])]
                     (if-not (= auth (str "Basic " (codec/base64-encode (.getBytes "foo:bar"))))
                       (-> context
                           terminate
                           (assoc-in [:response] {:status 403}))
                       context)))

(swagger/defhandler delete-db
                    {:summary "Delete db"}
                    [_]
                    (reset! job-store {})
                    {:status 204})

;;;; Routes

(def port (Integer. (or (System/getenv "PORT") 8000)))

(s/with-fn-validation
  (swagger/defroutes routes
                     {:title "Swagger Sample App"
                      :description "This is a sample jobstore server."
                      :version "2.0"}
                     [[["/" ^:interceptors [(body-params/body-params)
                                            bootstrap/json-body
                                            (swagger/body-params)
                                            (swagger/keywordize-params :form-params :headers)
                                            (swagger/coerce-params)
                                            (swagger/validate-response)]
                        ["/jobs"
                         {:get get-all-jobs}
                         {:post add-job}
                         ["/:id" ^:interceptors [load-job-from-db]
                          {:get get-job-by-id}
                          {:put update-job}
                          ]]
                        ;; security?
                        ;     ["/secure" ^:interceptors [basic-auth] {:delete delete-db}]

                        ["/doc" {:get [(swagger/swagger-doc)]}]
                        ["/*resource" {:get [(swagger/swagger-ui)]}]]]]))

(def service {:env :prod
              ::bootstrap/routes routes
              ::bootstrap/resource-path "/public"
              ::bootstrap/type :jetty
              ::bootstrap/port port})