;;; idf.el --- Incremental dataflows in emacs -*- lexical-binding: t -*-

;; Author: Boris Glavic <lordpretzel@gmail.com>
;; Maintainer: Boris Glavic <lordpretzel@gmail.com>
;; Version: 0.1
;; Package-Requires: (cl-lib cl-generic dash ht sorted-list avl-tree)
;; Homepage: https://github.com/lordpretzel/idf
;; Keywords:


;; This file is not part of GNU Emacs

;; This file is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3, or (at your option)
;; any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; For a full copy of the GNU General Public License
;; see <http://www.gnu.org/licenses/>.

;;; Commentary:

;; An implementation of incremental and lazy dataflows in Emacs inspired by the
;; likes of Spark (without the distribution).

;;; Code:

;; ********************************************************************************
;; IMPORTS
(require 'cl-lib)
(require 'cl-generic)
(require 'dash)
(require 'ht)
(require 'sorted-list)
(require 'avl-tree)

;; ********************************************************************************
;; CUSTOM

;;TODO add checks, e.g., for incorrect schemas

;; ********************************************************************************
;; FUNCTIONS - UTILITIES
(defsubst idf-ht-inc (ht key)
  "Increment the number associated as value in HT with KEY."
  (ht-set ht key (1+ (ht-get ht key 0))))

(defsubst idf-ht-dec (ht key &optional allow-negative)
  "Descrement the number associated as value in HT with KEY.

If ALLOW-NEGATIVE is non-nil, then do not allow negative values
by not decreasing counts that are 0."
  (ht-set ht key
          (if allow-negative
              (1- (ht-get ht key 0))
            (max (1- (ht-get ht key 0)) 0))))

(defsubst idf-ht-add (ht key number)
  "Add NUMBER to the value associated by hashtable HT with KEY."
  (ht-set ht key (+ number (ht-get ht key 0))))

;; ********************************************************************************
;; GENERIC FUNCTIONS FOR DATAFRAMES
(cl-defgeneric idf-collect (dataset)
  "Collect method for datasets.

Evaluate the dataflow graph described by DATASET and returns
its result.")

(cl-defgeneric idf-materialize (dataset)
  "Materialize the result of the DATASET.

Also prepares data structures for incrementally maintaining the result.")

(cl-defgeneric idf-apply-delta (dataset inputdelta)
  "Incrementally maintain the DATASET result.

Use INPUTDELTA (a set of rows to insert and a set of rows to
delete) to compute the delta for DATASET's result.")

(cl-defgeneric idf-prepare-for-maintenance (dataset)
  "Create state for DATASET for future incremental updating.")

;; ********************************************************************************
;; DATA STRUCTURES - DATAFRAMES AND DELTAS
(cl-defstruct (idf--ds
               (:constructor idf--ds-create))
  "Dataset: a dataflow graph whose result is lazily created."
  (result nil :type (list plist) :documentation "Cached result for the dataframe.")
  (type nil :documentation "The type of objects stored in the dataset. Use nil if unknown.")
  (setup-for-ivm nil :type boolean :documentation "If true, then the dataframe is ready for incremental maintenance.")
  inputs)

(cl-defstruct (idf--df
               (:include idf--ds (type 'plist))
               (:constructor idf--df-create))
  "Data frame (a dataflow graph whose result is lazily created."
  (schema nil :type (list symbol) :documentation "Data frame's schema (list of symbols)"))

(cl-defstruct (idf-delta
               (:constructor idf-delta-create))
  "Represents updates to a dataframe as a set of inserted and a set of deleted row."
  (inserted nil :type list)
  (deleted nil :type list))

(defmacro with-delta (delta &rest body)
  "Given DELTA execute BODY making inserted and deleted tuples available.

With the inserted tuples of the delta available as `ins' and the
deleted tuples available as `del'."
  (declare (indent defun))
  `(let ((ins (idf-delta-inserted ,delta))
         (del (idf-delta-deleted ,delta)))
     (progn ,@body)))

;; ********************************************************************************
;; DATA STRUCTURES - DATA SOURCES

(cl-defstruct (idf--source
               (:include idf--df)
               (:constructor idf-source--create))
  "Superclass of all data sources (lists of plists)."
  (name nil :type symbol :documentation "Name to identify a datasource."))

;; Literal source (a constant dataframe)
(cl-defstruct (idf-source-literal
               (:include idf--source)
               (:constructor idf-source-literal-create))
  "Datasource fix fixed content."
  (content () :type list))

(cl-defmethod idf-collect ((df idf-source-literal))
  (idf-source-literal-content df))

(cl-defmethod idf-materialize ((df idf-source-literal))
  (setf (idf--df-result df) (idf-source-literal-content df)))

(cl-defmethod idf-prepare-for-maintenance ((df idf-source-literal))
  (setf (idf--df-setup-for-ivm df) t))

(cl-defmethod idf-apply-delta ((df idf-source-literal) (inputdelta idf-delta))
  "Just return the delta passed for this source."
  inputdelta)

;; Source whose content and deltas are computed by calling functions
(cl-defstruct (idf-source-fn
               (:include idf--source)
               (:constructor idf-source-fn-create))
  (generator-fn nil :type function)
  (update-fn nil :type function :documentation "Function that fetches updates to the source represented as a delta."))

(cl-defmethod idf-collect ((df idf-source-fn))
  (funcall (idf-source-fn-generator-fn df)))

(cl-defmethod idf-materialize ((df idf-source-fn))
  (setf (idf--df-result df) (funcall (idf-source-fn-generator-fn df))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-source-fn))
    (setf (idf--df-setup-for-ivm df) t))

(cl-defmethod idf-apply-delta ((df idf-source-fn) (_ list))
  (setf (idf--df-result df) (funcall (idf-source-fn-update-fn df))))

;; ********************************************************************************
;; DATA STRUCTURES - OPERATORS
(cl-defstruct (idf--operator
               (:include idf--df)
               (:constructor idf--operator))
  (execut-fn nil :type function :documentation "Implementation of this operator."))

;; MAP OPERATOR
(cl-defstruct (idf-op-map
               (:include idf--operator)
               (:constructor idf--op-map-create))
  (map-fn 'identify :type function :documentation "The map function that is applied to every row (plist) of the dataframe"))

(cl-defmethod idf-collect ((df idf-op-map))
  (-map (idf-op-map-map-fn df)
        (idf-collect (car (idf-op-map-inputs df)))))

(cl-defmethod idf-materialize ((df idf-op-map))
  (setf (idf--df-result df) (idf-collect df)))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-map))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
    (setf (idf--df-setup-for-ivm df) t))

(cl-defmethod idf-apply-delta ((df idf-op-map) (inputdelta list))
  (let ((fn (idf-op-map-map-fn df)))
    (with-delta (car inputdelta)
                (idf-delta-create
                 :inserted (-map fn ins)
                 :deleted (-map fn del)))))

;; FILTER OPERATOR
(cl-defstruct (idf-op-filter
               (:include idf--operator)
               (:constructor idf--op-filter-create))
  (predicate-fn nil :type function :documnetation "This function is evaluated for every input and tuples for which it returns non-nil are filtered out."))

(cl-defmethod idf-collect ((df idf-op-filter))
  (-filter (idf-op-filter-predicate-fn df)
        (idf-collect (car (idf-op-filter-inputs df)))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-filter))
    (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
    (setf (idf--df-setup-for-ivm df) t))

(cl-defmethod idf-apply-delta ((df idf-op-filter) (delta list))
  (let ((fn (idf-op-filter-predicate-fn df)))
    (with-delta (car delta)
                (idf-delta-create
                 :inserted (-filter fn ins)
                 :deleted (-filter fn del)))))

;; UNION OPERATOR
(cl-defstruct (idf-op-union
               (:include idf--operator)
               (:constructor idf--op-union-create)))

(cl-defmethod idf-collect ((df idf-op-union))
  (append
   (idf-collect (car (idf-op-union-inputs df)))
   (idf-collect (cadr (idf-op-union-inputs df)))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-union))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
    (setf (idf--df-setup-for-ivm df) t))

(cl-defmethod idf-apply-delta ((df idf-op-union) (delta list))
  (idf-delta-create
   :inserted (append (idf-delta-inserted (car delta))
                     (idf-delta-inserted (cadr delta)))
   :deleted (append (idf-delta-deleted (car delta))
                     (idf-delta-deleted (cadr delta)))))

;; REDUCE OPERATOR
;;TODO implement grouping
(cl-defstruct (idf-op-reduce
               (:include idf--operator)
               (:constructor idf--op-reduce-create))
  (init-val nil :type plist :documentation "The initial value for the reducer function")
  (reduce-fn 'concat :type function :documentation "The reducer function")
  (group-fn nil :type (list symbol) :documentation "Group input on the result of this function and apply reduce to each group.")
  (reduce-results nil :type hashtable :documentation "Store for each group the reduce result.")
  (reduce-inputs nil :type hashtable :documentation "Store inputs each group for reduce to deal with deletes."))

(defun idf-ht-value-list-append (ht key value)
  "Append VALUE to the list stored at KEY in hashtable HT."
  (if (ht-contains-p ht key)
      (ht-set ht key (cons value (ht-get ht key)))
    (ht-set ht key (list value))))

(defun idf-ht-value-list-delete (ht key value)
  "Delete VALUE from the list stored at KEY in hashtable HT."
  (let ((newl (cl-remove value (ht-get ht key))))
    (if newl
        (ht-set ht key newl)
      (ht-remove ht key))))

(cl-defmethod idf-collect ((df idf-op-reduce))
  "Build hashtable with groups and lists of values, then apply reduce function to each list and return the result of DF."
  (let* ((grpfn (or (idf-op-reduce-group-fn df) (lambda (x) t)))
         (input (car (idf--df-inputs df)))
         (reduce (idf-op-reduce-reduce-fn df))
         (init-val (idf-op-reduce-init-val df))
         (ht (ht-create 'equal)))
    (--each (idf-collect input)
      (idf-ht-value-list-append ht (funcall grpfn it) it))
    (dolist (k (ht-keys ht))
      (let ((l (ht-get ht k)))
        (ht-set ht k (-reduce-from reduce init-val l))))
    (ht-values ht)))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-reduce))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
  (setf (idf--df-setup-for-ivm df) t)
  (let* ((grpfn (or (idf-op-reduce-group-fn df) (lambda (x) t)))
         (input (car (idf--df-inputs df)))
         (inputdata (idf-collect input))
         (reduce (idf-op-reduce-reduce-fn df))
         (init-val (idf-op-reduce-init-val df))
         (htinputs (ht-create 'equal))
         (htresults (ht-create 'equal)))
    (setf (idf-op-reduce-reduce-inputs df)
          htinputs)
    (setf (idf-op-reduce-reduce-results df)
          htresults)
    (--each inputdata
      (idf-ht-value-list-append htinputs (funcall grpfn it) it))
    (cl-loop for (k . l) in (ht->alist htinputs)
             do
        (ht-set htresults k (-reduce-from reduce init-val l)))))

(cl-defmethod idf-materialize ((df idf-op-reduce))
  (unless (idf--df-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (setf (idf--df-result df) (idf-collect df)))

(cl-defmethod idf-apply-delta ((df idf-op-reduce) (deltas list))
  (unless (idf--df-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (let ((htinputs (idf-op-reduce-reduce-inputs df))
        (htresults (idf-op-reduce-reduce-results df))
        (grpfn (or (idf-op-reduce-group-fn df) (lambda (x) t)))
        (reduce (idf-op-reduce-reduce-fn df))
        (init-val (idf-op-reduce-init-val df))
        (prevvalues (ht-create 'equal)))
    (with-delta (car deltas)
      ;; delete and insert into group inputs
      (dolist (d del)
        (let ((key (funcall grpfn d)))
          (unless (ht-contains-p prevvalues key)
            (ht-set prevvalues key (ht-get htresults key)))
          (idf-ht-value-list-delete htinputs key d)))
      (dolist (i ins)
        (let ((key (funcall grpfn i)))
          (unless (ht-contains-p prevvalues key)
            (ht-set prevvalues key (ht-get htresults key)))
          (idf-ht-value-list-append htinputs key i)))
      ;; compute new results
      (dolist (k (ht-keys prevvalues))
        (ht-set htresults k (-reduce-from reduce init-val (ht-get htinputs k))))
      ;; create delta
      (idf-delta-create
       :deleted (->>
                 (ht->alist prevvalues)
                 (-map 'cdr)
                 (--filter it))
       :inserted (->>
                  (ht-keys prevvalues)
                  (--map (cons it (ht-get htinputs it)))
                  (-map 'cdr)
                  (--filter it))))))

;; AGGREGATION
(cl-defstruct (idf-op-aggregate
               (:include idf--operator)
               (:constructor idf--op-aggregate-create))
  (group-bys nil :type (list string) :documentation "Names of group-by attributes")
  (agg-functions nil :type (alist) :documentation "Aggregation functions as alist (agg-fn-symbol attributename)")
  (delta-results nil :type hashtable :documentation "Store aggregation function results for each group to for incremental maintenance."))

(defun idf--agg-create-schema (groups aggs)
  "Create the schema of an aggregation result from the GROUPS and aggregation functions AGGS."
  (append
   groups
   (--map (intern (concat ":"
                          (symbol-name (car it))
                          "-"
                          (substring (symbol-name (cadr it)) 1)))
          aggs)))

(cl-defmethod idf-collect ((df idf-op-aggregate))
  "Do hash aggregation to collect results."
  (let* ((groups (idf-op-aggregate-group-bys df))
        (aggs (idf-op-aggregate-agg-functions df))
        (inputs (idf-collect (car (idf-op-aggregate-inputs df))))
        (schema (or (idf-op-aggregate-schema df)
                    (idf--agg-create-schema groups aggs))))
    (if groups
        ;; group-by
        (let ((aggresults (ht-create)))
          (dolist (r inputs)
            (idf--update-group groups aggs aggresults r))
          (ht-map (lambda (k v)
                    (idf--merge-schema-and-values schema (append k v)))
                  aggresults))
      ;; no group-by
      (let ((aggresults (idf--init-agg-results aggs)))
        (dolist (r inputs)
          (setq aggresults (idf--update-aggs aggresults aggs r)))
        (list (idf--merge-schema-and-values schema aggresults))))))

(defun idf--merge-schema-and-values (schema values)
  "Merge list of attributes SCHEMA with list of VALUES into a tuple plist."
  (-reduce 'append (-zip-lists schema values)))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-aggregate))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
  (setf (idf--df-setup-for-ivm df) t)
  (let ((inputresult (idf-collect (car (idf--df-inputs df)))))
    (if (idf-op-aggregate-group-bys df)
        (setf (idf-op-aggregate-delta-results df)
              (ht-create 'equal))
      (setf (idf-op-aggregate-delta-results df)
            (idf--init-agg-results (idf-op-aggregate-agg-functions df))))
    (idf-apply-delta df (list (idf-delta-create :inserted inputresult)))))

(cl-defmethod idf-materialize ((df idf-op-aggregate))
  (unless (idf--df-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (setf (idf--df-result df) (idf-collect df)))

(cl-defmethod idf-apply-delta ((df idf-op-aggregate) (inputdelta list))
  (unless (idf--df-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (with-delta (car inputdelta)
    (let* ((groups (idf-op-aggregate-group-bys df))
          (aggs (idf-op-aggregate-agg-functions df))
          (schema (or (idf-op-aggregate-schema df)
                      (idf--agg-create-schema groups aggs)))
          (ht (idf-op-aggregate-delta-results df)))
      (if groups
          ;; group-by, update aggregation results keeping track of old versions of updated groups
          (let ((oldgroupvalues (ht-create))
                (newgroupvalues (ht-create)))
            (dolist (r ins)
              (idf--maintain-ins-group groups aggs ht oldgroupvalues newgroupvalues r))
            (dolist (r del)
              (idf--maintain-del-group groups aggs ht oldgroupvalues newgroupvalues r))
            (idf-delta-create
             :inserted
             (ht-map (lambda (k v)
                       (idf--merge-schema-and-values schema (append k v)))
                     newgroupvalues)
             :deleted
             (ht-map (lambda (k v)
                       (idf--merge-schema-and-values schema (append k v)))
                     oldgroupvalues)))
        ;; no group-by
        (let* ((oldagg (idf-op-aggregate-delta-results df))
               (newagg oldagg))
          (dolist (r ins)
            (setq newagg (idf--ins-update-agg newagg aggs r)))
          (dolist (r del)
            (setq newagg (idf--del-update-agg newagg aggs r)))
          (setf (idf-op-aggregate-delta-results df) newagg)
          (idf-delta-create
           :inserted (list (idf--merge-schema-and-values schema newagg))
           :deleted (list (idf--merge-schema-and-values schema oldagg))))))))

(defun idf--update-aggs (cur aggs tuple)
  "Update aggregation function state CUR for AGGS with values from TUPLE."
  (->> (-zip-lists cur aggs)
       (--map
        (let* ((agg (caadr it))
               (a (cadadr it))
               (newval (plist-get tuple a))
               (val (car it)))
        (pcase agg
           ('sum (+ val newval))
           ('avg (cons (+ (car val) 1) (+ (cadr val) newval)))
           ('min (min val newval))
           ('max (max val newval)))))))

(defun idf--ins-update-agg (cur aggs tuple)
  "Maintain aggregation result CUR for AGGS by adding values from TUPLE."
  (->> (-zip-lists cur aggs)
       (--map
        (let* ((agg (caadr it))
               (a (cadadr it))
               (newval (plist-get tuple a))
               (val (car it)))
          (pcase agg
            ('sum (+ val newval))
            ('avg (cons (- (car val)  1) (- (cadr val) newval)))
            ('min (avl-tree-enter val newval))
            ('max (avl-tree-enter val newval)))))))

(defun idf--del-update-agg (cur aggs tuple)
  "Update aggregation result CUR for AGGS by deducting values from TUPLE."
  (->> (-zip-lists cur aggs)
       (--map
        (let* ((agg (caadr it))
               (a (cadadr it))
               (newval (plist-get tuple a))
               (val (car it)))
          (pcase agg
            ('sum (+ val newval))
            ('avg (cons (- (car val)  1) (- (cadr val) newval)))
            ('min (avl-tree-delete val newval))
            ('max (avl-tree-delete val newval)))))))

(defun idf--maintain-ins-group (groups aggs ht oldgroupvalues newgroupvalues tuple)
  "Maintain aggregtion result AGGS for GROUPS stored in hashtable HT by inserting TUPLE."
  (let* ((groups (--map (plist-get tuple it) groups))
         (state (ht-get ht groups))
         newstate)
    (if state
        (unless (ht-contains-p oldgroupvalues groups)
          (ht-set oldgroupvalues groups state))
      (setq state (idf--init-delta-agg-results aggs)))
    (setq newstate (idf--ins-update-agg state aggs tuple))
    (ht-set ht groups newstate)
    (ht-set newgroupvalues groups newstate)))

(defun idf--maintain-del-group (groups aggs ht oldgroupvalues newgroupvalues tuple)
  "Maintain aggregtion result AGGS for GROUPS stored in hashtable HT by deleting TUPLE."
  (let* ((groups (--map (plist-get tuple it) groups))
         (state (ht-get ht groups))
         newstate)
    (unless state
      (error "Something went wrong, tried to delete tuple from a non-existing group"))
    (unless (ht-contains-p oldgroupvalues groups)
      (ht-set oldgroupvalues groups state))
    (setq newstate (idf--del-update-agg state aggs tuple))
    (ht-set ht groups newstate)
    (ht-set newgroupvalues groups newstate)))

(defun idf--update-group (groups aggs ht tuple)
  "Update partial aggregation result for AGGS for GROUPS of TUPLE stored in hashtable HT."
  (let* ((groups (--map (plist-get tuple it) groups))
         (state (ht-get ht groups)))
    (unless state
      (setq state (idf--init-agg-results aggs)))
    (ht-set ht groups
            (idf--update-aggs state aggs tuple))))

(defun idf--init-agg-results (aggs)
  "Intialize aggregation state datastructures for AGGS."
  (--map (pcase (car it)
           ('sum 0)
           ('avg '(0 0))
           ('min most-positive-fixnum)
           ('max most-negative-fixnum))
         aggs))

(defun idf--init-delta-agg-results (aggs)
  "Intialize aggregation state datastructures for incrementally maintaining AGGS."
  (--map (pcase (car it)
           ('sum 0)
           ('avg '(0 0))
           ('min (avl-tree-create '<))
           ('max (avl-tree-create '>)))
         aggs))

;; UNIQ OPERATOR
(cl-defstruct (idf-op-unique
               (:include idf--operator)
               (:constructor idf--op-unique-create))
  (delta-elements nil :type hashmap :documentation "Store for each tuple a count to be able to handle updates.")
  (keyfn nil :type function :documentation "Use this function to extract keys from tuples."))

(defun idf-cmp-fn-from-key-extractor (keyfn)
  "Create an equality function for a key extractor function."
  `(lambda (a b) (equal (,keyfn a) (,keyfn b))))

(cl-defmethod idf-collect ((df idf-op-unique))
  (let* ((keyfn (idf-op-unique-keyfn df))
         (cmpfn (idf-cmp-fn-from-key-extractor keyfn))
        (in (car (idf-op-unique-inputs df))))
    (if keyfn
        (let ((-compare-fn cmpfn))
          (-uniq (idf-collect in)))
      (-uniq (idf-collect (car (idf-op-unique-inputs df)))))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-unique))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
  (let* ((keyfn (idf-op-unique-keyfn df))
         (eqfn (if keyfn
                   (idf-cmp-fn-from-key-extractor keyfn)
                 'equal))
         (ht (make-hash-table :test eqfn)))
    (setf (idf--df-setup-for-ivm df) t)
    (setf (idf-op-unique-delta-elements df)
        ht)))

(cl-defmethod idf-materialize ((df idf-op-unique))
  (unless (idf-op-unique-delta-elements df)
    (idf-prepare-for-maintenance df))
  (let* ((ht (idf-op-unique-delta-elements df))
         (keyfn (or (idf-op-unique-keyfn df) 'identity))
        (results (idf-collect df)))
    (setf (idf--df-result df) results)
    (dolist (r results)
      (idf-ht-inc ht (funcall keyfn r)))))

(cl-defmethod idf-apply-delta ((df idf-op-unique) (delta list))
  (with-delta
   (car delta)
   (let* ((keyfn (idf-op-unique-keyfn df))
          (eqfn (if keyfn
                    (idf-cmp-fn-from-key-extractor keyfn)
                  'equal))
          (realdelta (make-hash-table))
          (outins nil)
          (outdel nil))
     (--each ins (idf-ht-inc realdelta it))
     (--each del (idf-ht-dec realdelta it t))
     (ht-amap
      (cond ((> value 0)
             (add-to-list outins (-repeat value key)))
            ((< value 0)
             (add-to-list outdel (-repeat value key))))
      realdelta)
     (idf-delta-create
      :inserted outins
      :deleted outdel))))

;; ********************************************************************************
;; MV
(cl-defstruct (idf-mv
               (:include idf--df)
               (:constructor idf-mv--create))
  (mvtype 'hashtable :type symbol :documentation "What data structure is used to store the materialized view? `hashtable' or sorted `list'")
  (cmpfn nil :type symbol :documentation "When storing as a sorted lists, then use this function as the small-then function. For hashtables this is the hash test (created with `define-hash-table-test'")
  (keyfn nil :type function :documentation "For hashtables use this function to extract a key from a tuple.")
  (valuefn nil :type function :documentation "For hashtables use this function to extract the value from a tuple.")
  (store-as-symbol nil :type symbol :documentation "Store mv as this symbol.")) ;;TODO not supported yet

(cl-defmethod idf-collect ((df idf-mv))
(idf-collect (car (idf--df-inputs df))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-mv))
  (dolist (i (idf--ds-inputs df))
    (idf-prepare-for-maintenance i))
  (unless (idf--ds-setup-for-ivm df)
    (setf (idf--ds-setup-for-ivm df) t)
    (pcase (idf-mv-mvtype df)
      ('hashtable
       (setf (idf--df-result df)
             (ht-create (idf-mv-cmpfn df))))
      ('sortedlist
       (setf (idf--df-result df)
             (sorted-list-create nil (idf-mv-cmpfn df)))))))

(cl-defmethod idf-materialize ((df idf-mv))
  (unless (idf--ds-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (let ((inputds (car (idf--ds-inputs df)))
        (storesym (idf-mv-store-as-symbol df)))
    (pcase (idf-mv-mvtype df)
      ('hashtable
       (let* ((input (idf-collect inputds))
             (keyfn (idf-mv-keyfn df))
             (valuefn (idf-mv-valuefn df))
             (cmpfn (idf-mv-cmpfn df))
             (ht (idf--df-result df)))
         (dolist (i input)
           (ht-set ht (funcall keyfn i) (funcall valuefn i)))))
      ('sortedlist
       (let ((input (idf-collect inputds)))
         (setf (idf--ds-result df)
               (sorted-list-create input (idf-mv-cmpfn df)))
         (when storesym
           (set storesym (sorted-list-list (idf--ds-result df)))))))))

(cl-defmethod idf-apply-delta ((df idf-mv) (delta list))
  "Apply DELTA to materialized view IDF-MV and return DELTA."
  (pcase (idf-mv-mvtype df)
    ('hashtable
     (let ((ht (idf--df-result df))
           (keyfn (idf-mv-keyfn df))
           (valuefn (idf-mv-valuefn df)))
       (with-delta (car delta)
         (--each del (ht-remove ht (funcall keyfn it)))
         (--each ins (ht-set ht (funcall keyfn it) (funcall valuefn it))))
         (car delta))) ;;FIXME hashtable behaves all unique so adapt the delta
    ('sortedlist
     (let ((sl (idf--df-result df)))
       (with-delta (car delta)
         (--each del (sorted-list-delete sl it))
         (--each ins (sorted-list-insert sl it)))
       (car delta)))))

(defun idf-mv-create-smallerfn (sortkeys &optional types)
  "Create a function comparing two plists on SORTKEYS whether the first one is smaller than the second one."
  `(lambda (a b)
    (--all-p (< (plist-get a it) (plist-get b it)) ',sortkeys)))

(defun idf-create-extractor (attrs)
  "Create a function that projects tuples on ATTRS."
  (if attrs
      (if (> (length attrs) 1)
          `(lambda (x) (--map (plist-get x it) ',attrs))
        `(lambda (x) (plist-get x ',(car attrs))))
    `(lambda (x) t)))

(defun idf-mv-get-result (mv)
  "Get the materialized result of a materialized view."
  (let ((r (idf-mv-result mv)))
    (pcase (idf-mv-mvtype mv)
      ('sortedlist (sorted-list-list r))
      ('hashtable r))))

;; ********************************************************************************
;; FUNCTIONS - UTILITY
(defun idf-plistp (l)
  "Test whether L may be a plist."
  (if (not (and (listp l) (equal (mod (length l) 2) 0)))
      nil
    (if (cl-loop for (key value) on l by #'cddr
                   when (not (symbolp key))
                   return t)
        nil
      t)))

;; ********************************************************************************
;; FUNCTIONS - INCREMENTAL MAINTENANCE
(cl-defun idf-materialize-as (df &key viewtype schema maintain-as-symbol comparefn sortkeys keyfn keyattrs valuefn valueattrs)
  "Materialize DF as VIEWTYPE (`'hashtable' or `'sortedlist').

Either COMPAREFN or SORTKEYS have to be specified to determine
the sort order for `'sortedlist' views. Optionally, a SCHEMA for
the result dataframe can be provided."
  (let ((smallerfn (or comparefn (when sortkeys (idf-mv-create-smallerfn sortkeys))))
        (keyfn (or keyfn (when keyattrs (idf-create-extractor keyattrs))))
        (valuefn (or valuefn (idf-create-extractor valueattrs)))
        (schema (or schema (when (idf--df-p df) (idf--df-schema df))))
        themv)
    (setq themv
          (idf-mv--create
           :mvtype viewtype
           :type (idf--df-type df)
           :schema schema
           :inputs `(,df)
           :store-as-symbol maintain-as-symbol
           :cmpfn smallerfn
           :keyfn keyfn
           :valuefn valuefn))
    (idf-materialize themv)
    themv))

(defun idf-maintain (df updates)
  "Incrementally maintain materialized dataframe DF based on UPDATES.

We return the delta for DF generated by the maintenance. For
literal datasources (`idf-source-literal') updates have to be
provided explicitly as parameter UPDATES. For function data
sources (`idf-source-fn') , we will call
`idf-source-fn-update-fn'."
    (unless (idf--df-setup-for-ivm df)
      (idf-prepare-for-maintenance df))
    (cond
     ;; for literal sources apply update (if it exists)
     ((idf-source-literal-p df)
      (let* ((name (idf--source-name df)))
        (or (plist-get updates name) (idf--empty-delta))))
     ;; for function sources call the update function
     ((idf-source-fn-p df)
      (funcall (idf-source-fn-update-fn df)))
     ;; operator
     (t
      (let ((input-updates (--map (idf-maintain it updates) (idf--df-inputs df))))
        (idf-apply-delta df input-updates)))))

(defun idf-maintain-multiple (dfs updates &optional delta-cache)
  "Given list of dataframes DFS, apply UPDATES.

WE incrementally maintain the results for all these dataframes.
Importantly, operators in the dataflow graphs may be shared
across the dataframes and we take care to only update the result
of each operator once."
  (unless delta-cache
    (setq delta-cache (ht-create 'equal)))
  (dolist (df dfs)
    (unless (idf--df-setup-for-ivm df)
      (idf-prepare-for-maintenance df))
    ;; maintain inputs
    (--each (idf--df-inputs df)
      (idf-maintain-multiple (list it) updates delta-cache))
    (unless (ht-get delta-cache df)
      (puthash df
               (cond
                ;; for literal sources apply update (if it exists)
                ((idf-source-literal-p df)
                 (let* ((name (idf--source-name df)))
                   (or (plist-get updates name) (idf--empty-delta))))
                ;; for function sources call the update function
                ((idf-source-fn-p df)
                 (funcall (idf-source-fn-update-fn df)))
                ;; operator
                (t
                 (let ((input-updates (--map (ht-get delta-cache it) (idf--df-inputs df))))
                   (idf-apply-delta df input-updates))))
               delta-cache))))

(defun idf--empty-delta ()
  "Create an emply `idf-delta'."
  (idf-delta-create
   :inserted nil
   :deleted nil))

;; ********************************************************************************
;; FUNCTIONS - DATAFRAME OPERATIONS
(cl-defun idf-map (df mapfn &key resultschema type)
  "Create dataframe that maps MAPFN to input DF.

If RESULTSCHEMA is provided, then use it as the schema for the
returned dataframe. For datasets provide TYPE to store the type
of result. Otherwise, keep schema undecided (nil)."
  (let ((schema (or resultschema nil))
        (type (or type (when resultschema 'plist))))
    (idf--op-map-create
     :schema schema
     :type type
     :inputs `(,df)
     :map-fn mapfn)))

(cl-defun idf-reduce (df &key reducefn groupfn groupattrs initval type resultschema)
  "Create dataframe DF that applies REDUCEFN to merge input into a single row.

If INITVAL is non-nil, then initialize the result to INITVAL. If
RESULTSCHEMA is provided then rename attributes accordingly. If
GROUP-BY is non-nil, then group the rows of the dataframe on
these columns and apply the reducer to every group."
  (idf--op-reduce-create
   :reduce-fn reducefn
   :group-fn (or groupfn (idf-create-extractor groupattrs))
   :init-val initval
   :type (if resultschema 'plist (or type 'list))
   :schema resultschema
   :inputs `(,df)))

(cl-defun idf-create-source (&key name content generatorfn updatefn schema)
  (when (and (not content) (not generatorfn))
    (error "sources are either created from literal content or from a generator functions"))
  (if content
      (idf-source-literal-create
       :name name
       :content content
       :schema (or schema
                   (when (idf-plistp (car content))
                       (-slice (car content) 0 nil 2))))
    (idf-source-fn-create
     :name name
     :schema schema
     :generator-fn generatorfn
     :update-fn updatefn)))

(cl-defun idf-project (df &key attrs resultschema)
  "Project a dataframe DF on ATTRS.

if RESULTSCHEMA is provided, then rename attributes like this."
  (idf--op-map-create
   :schema (or resultschema attrs)
   :inputs `(,df)
   :map-fn (lambda (tuple)
             (-flatten
              (--map (list it (plist-get tuple it))
                     attrs)))))

(cl-defun idf-filter (df &key expr fn)
  "Filter the rows of DF.

If EXPR is provided then generate a function based on the EXPR.
Any symbol $NAME in EXPR will be replaced with the value of the
attribute NAME, e.g., `(< $A $B)` would filter out all rows where
the value of attribute A is smaller than the one of attribute B.
If FN is provided the evaluate FN on very input row and if it
returns nil, then filter out the row."
  (idf--op-filter-create
   :schema (idf--df-schema df)
   :inputs `(,df)
   :predicate-fn (if fn
                     fn
                   (idf-expr-to-lambda expr))))

(defun idf-union (leftdf rightdf &optional schema)
  "Union LEFTDF and RIGHTDF."
  (idf--op-union-create
   :schema (or schema (idf--df-schema leftdf))
   :type (idf--df-type leftdf)
   :inputs `(,leftdf ,rightdf)))

(cl-defun idf-unique (df &key schema keyextractor)
  "Remove duplicates from DF.

If KEYEXTRACTOR is used extract a key from tuples to determine
equality for duplicate elimination."
  (idf--op-unique-create
   :type (idf--df-type df)
   :schema (or schema (idf--df-schema df))
   :inputs `(,df)
   :keyfn keyextractor))

(cl-defun idf-aggregate (df &key group-by aggs schema)
  "Group DF on GROUP-BY attributes and then compute aggregation functions AGGS for each group."
  (idf--op-aggregate-create
   :type 'plist
   :schema (or schema (idf--agg-create-schema group-by aggs))
   :group-bys group-by
   :agg-functions aggs
   :inputs `(,df)))

(defun idf-expr-to-lambda (expr)
  "Return a lambda that evaluates EXPR over a tuple (a plist)."
  `(lambda (tup)
     ,(idf--expr-to-code expr)))

(defun idf--expr-to-code (expr)
  "Construct list code from EXPR.

Currently, the only thing we do is to replace symbols $NAME with
code that extracts the attribute NAME from an input tuple."
  (cond ((and (symbolp expr) (string-prefix-p "$" (symbol-name expr)))
         `(plist-get tup ,(intern (concat ":" (substring (symbol-name expr) 1)))))
        ((listp expr)
         (--map (idf--expr-to-code it) expr))
        (t expr)))

(provide 'idf)
;;; idf.el ends here
