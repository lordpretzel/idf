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
;; likes of Spark (without the distribution of course).

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
(cl-defstruct (idf--df
               (:constructor idf--df-create))
  "Data frame: a dataflow graph whose result is lazily created."
  (result nil :type list :documentation "Cached result for the dataframe.")
  (type nil :documentation "The type of objects stored in the dataset. Use nil if unknown.")
  (setup-for-ivm nil :type boolean :documentation "If true, then the dataframe is ready for incremental maintenance.")
  inputs
  (schema nil :type (list symbol) :documentation "Data frame's schema (list of symbols). Only for dataframes of type plist"))

(cl-defstruct (idf-delta
               (:constructor idf-delta-create))
  "Represents updates to a dataframe as a set of inserted and a set of deleted row."
  (inserted nil :type list)
  (deleted nil :type list))

(defun idf-delta-size (delta)
  "Return the number of deleted and inserted tuples for DELTA."
  (+ (length (idf-delta-inserted delta))
     (length (idf-delta-deleted delta))))

(defmacro with-delta (delta &rest body)
  "Given DELTA execute BODY making inserted and deleted tuples available.

With the inserted tuples of the delta available as `ins' and the
deleted tuples available as `del'."
  (declare (indent defun))
  `(let ((ins (idf-delta-inserted ,delta))
         (del (idf-delta-deleted ,delta)))
     (progn ,@body)))

(defmacro with-binary-delta (delta &rest body)
  "Given DELTA make deltas available for inputs of binary op.

Execute BODY with binding inserts and deletes to `l-ins',
`l-del', `r-ins', `r-del'."
  (declare (indent defun))
  `(let ((l-ins (idf-delta-inserted (car ,delta)))
         (l-del (idf-delta-deleted (car ,delta)))
         (r-ins (idf-delta-inserted (cadr ,delta)))
         (r-del (idf-delta-deleted (cadr ,delta))))
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
  (ignore df)
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
  (ignore df)
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
  (reduce-results nil :type hashtable :documentation "Store for each group of the reduce result.")
  (reduce-inputs nil :type hashtable :documentation "Store inputs of each group for reduce to deal with deletes."))

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

(defun idf-one-group-fn (x)
  "Return t, used to group all inputs into one group.

Ignores input X."
  (ignore x)
  t)

(cl-defmethod idf-collect ((df idf-op-reduce))
  "Build hashtable with groups and lists of values.

Then apply reduce function to each list and return the result of DF."
  (let* ((grpfn (or (idf-op-reduce-group-fn df) #'idf-one-group-fn))
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
  (let* ((grpfn (or (idf-op-reduce-group-fn df) #'idf-one-group-fn))
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
        (grpfn (or (idf-op-reduce-group-fn df) #'idf-one-group-fn))
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
  (delta-results nil :type hashtable :documentation "Store aggregation  results for incremental maintenance."))

(defun idf--agg-create-schema (groups aggs)
  "Create the schema of an aggregation result.

The schema is the GROUPS followed by the aggregation function
results for AGGS."
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
                       (idf--merge-schema-and-values schema
                                                     (append k (plist-get v :agg))))
                     newgroupvalues)
             :deleted
             (ht-map (lambda (k v)
                       (idf--merge-schema-and-values schema
                                                     (append k (plist-get v :agg))))
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
            ('sum (- val newval))
            ('avg (cons (- (car val)  1) (- (cadr val) newval)))
            ('min (avl-tree-delete val newval))
            ('max (avl-tree-delete val newval)))))))

(defun idf--maintain-ins-group (groups aggs ht oldgroupvalues newgroupvalues tuple)
  "Maintain aggregtion result AGGS for GROUPS.

The results should be stored in hashtable HT. We update the
results by inserting TUPLE."
  (let* ((group (--map (plist-get tuple it) groups))
         (state (ht-get ht group))
         newstate)
    (if state
        (unless (ht-contains-p oldgroupvalues group)
          (ht-set oldgroupvalues group state))
      (setq state `(:agg ,(idf--init-delta-agg-results aggs) :count 0)))
    (setq newstate `(:agg ,(idf--ins-update-agg (plist-get state :agg) aggs tuple)
                          :count ,(1+ (plist-get state :count))))
    (ht-set ht group newstate)
    (ht-set newgroupvalues group newstate)))

(defun idf--maintain-del-group (groups aggs ht oldgroupvalues newgroupvalues tuple)
  "Maintain aggregtion result AGGS for GROUPS.

The results are expceted to be stored in hashtable HT. We
maintaint the result by deleting TUPLE."
  (let* ((group (--map (plist-get tuple it) groups))
         (state (ht-get ht group))
         (cnt (plist-get state :count))
         newstate)
    (unless state
      (error "Something went wrong, tried to delete tuple from a non-existing group"))
    (unless (ht-contains-p oldgroupvalues group)
      (ht-set oldgroupvalues group state))
    ;; if last element in the group, then remove the group
    (if (equal cnt 1)
        (progn
          (ht-remove ht group)
          (ht-remove newgroupvalues group))
      ;; else update the agg and reduce the count
      (setq newstate `(:agg ,(idf--del-update-agg (plist-get state :agg) aggs tuple)
                            :count ,(- cnt 1)))
    (ht-set ht group newstate)
    (ht-set newgroupvalues group newstate))))

(defun idf--update-group (groups aggs ht tuple)
  "Update partial aggregation result of TUPLE for AGGS for GROUPS.

The partial aggregation results are supposed to be stored in
hashtable HT."
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
  (keyfn 'identity :type function :documentation "Use this function to extract keys from tuples. By default this is `identity'."))

(defun idf-cmp-fn-from-key-extractor (keyfn)
  "Create an equality function for a key extractor function."
  (if (eq keyfn 'identify)
      'equal
      `(lambda (a b) (equal (,keyfn a) (,keyfn b)))))

(cl-defmethod idf-collect ((df idf-op-unique))
  (let* ((keyfn (idf-op-unique-keyfn df))
         (cmpfn (idf-cmp-fn-from-key-extractor keyfn))
         (in (car (idf-op-unique-inputs df))))
    (let ((-compare-fn cmpfn))
      (-uniq (idf-collect in)))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-unique))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
  (let* ((ht (ht-create 'equal)))
    (setf (idf--df-setup-for-ivm df) t)
    (setf (idf-op-unique-delta-elements df)
        ht)))

(cl-defmethod idf-materialize ((df idf-op-unique))
  (unless (idf-op-unique-delta-elements df)
    (idf-prepare-for-maintenance df))
  (let* ((ht (idf-op-unique-delta-elements df))
         (keyfn (idf-op-unique-keyfn df))
         (results (idf-collect df)))
    (setf (idf--df-result df) results)
    (dolist (r results)
      (idf-ht-inc ht (funcall keyfn r)))))

(cl-defmethod idf-apply-delta ((df idf-op-unique) (delta list))
  (with-delta
   (car delta)
   (let* ((keyfn (idf-op-unique-keyfn df))
          (ht (idf-op-unique-delta-elements df))
          (realdelta (ht-create 'equal))
          (outins nil)
          (outdel nil))
     (--each ins (idf-ht-inc realdelta (funcall keyfn it)))
     (--each del (idf-ht-dec realdelta (funcall keyfn it) t))
     (ht-amap
      (let* ((oldvalue (ht-get ht key))
             (newvalue (+ oldvalue value)))
        (ht-set ht key newvalue)
        (when (and (equal oldvalue 0) (> newvalue 0))
          (add-to-list outins key))
        (when (and (> oldvalue 1) (equal newvalue 0))
             (add-to-list outdel key)))
      realdelta)
     (idf-delta-create
      :inserted outins
      :deleted outdel))))

;; JOIN OPERATOR
(cl-defstruct (idf-op-join
               (:include idf--operator)
               (:constructor idf--op-join-create))
  (left-key-extractor nil :type function :documentation  "Function used to extract a key value from the left input")
  (right-key-extractor nil :type function :documentation "Function used to extract a key value from the right input")
  (match-fn 'equal :type function :documentation "Function used to decide whether two keys match")
  (merge-fn 'append :type function :documentation "Function to apply to two input to produced the joined result for these inputs")
  (join-type 'inner :type symbol :documentation "Type of join `inner', `left-outer', or `full-outer'")
  (left-result nil :documentation "Cache left result")
  (right-result nil :documentation "Cache right result"))

(defun idf--join-create-schema (left-df right-df)
  "Generate a schema for a join of LEFT-DF and RIGHT-DF."
  (let ((left-schema (cl-copy-list (idf--df-schema left-df)))
        (right-schema (cl-copy-list (idf--df-schema right-df))))
    (append left-schema right-schema)))

(cl-defun idf--hash-join (ht keyfn other mergefn other-is-right &key join-type left-attrs right-attrs right-ht) ;; right-keyfn)
  "Join rows from OTHER with rows in HT.

Apply KEYFN to rows from OTHER to extract join keys. Merge joined
inputs with MERGEFN. If OTHER-IS-RIGHT then use rows from other
as the right input to MERGEFN. Otherwise, use them as left input.
If JOIN-TYPE is provided (only `inner', `left-outer', and
`full-outer' are aollowed), then use this type of join. For an
`full-outer' join, a hash table for the other input needs to be
provided."
  (pcase (or join-type 'inner)
    ;; inner join
    ('inner
     (->> other
          (--map
           (-map (lambda (s)
                   (if other-is-right
                       (funcall mergefn s it)
                     (funcall mergefn it s)))
                 (ht-get ht (funcall keyfn it))))
          (--filter it)
          (apply 'append)))
    ('left-outer
     (let* ((nulls (make-list (length right-attrs) nil))
            (nulls-right (-interleave right-attrs nulls)))
       (->> other
            (--map
             (let ((join-partners
                    (-map (lambda (s)
                            (if other-is-right
                                (funcall mergefn s it)
                              (funcall mergefn it s)))
                          (ht-get ht (funcall keyfn it)))))
               (or join-partners
                   (list
                    (if other-is-right
                        (funcall mergefn (copy-sequence nulls-right) it)
                      (funcall mergefn it (copy-sequence nulls-right)))))))
            (apply 'append))))
    ;; full outer
    ('full-outer
     (let ((nulls-left (-interleave
                        left-attrs
                        (make-list (length left-attrs) nil)))
           (nulls-right (-interleave
                         right-attrs
                         (make-list (length right-attrs) nil)))
           (ht-right-has-jp (ht-create 'equal)))
       (append
        (->> other
             (--map
              (let ((join-partners
                     (-map (lambda (s)
                             (progn
                               (ht-set ht-right-has-jp s 1)
                               (if other-is-right
                                   (funcall mergefn s it)
                                 (funcall mergefn it s))))
                             (ht-get ht (funcall keyfn it)))))
                (or join-partners
                    (list
                     (funcall mergefn it (copy-sequence nulls-right))))))
             (apply 'append))
        (->> (apply 'append (ht-values right-ht))
             (--filter (not (ht-contains-p ht-right-has-jp it)))
             (--map (funcall mergefn (copy-sequence nulls-left) it))))))))

(defun idf--nested-loop-join (left right lkeyfn rkeyfn matchfn mergefn)
  "Nested loop join LEFT and RIGHT.

Extract keys using LKEYFN and RKEYFN and test matching with MATCHFN.
Apply MERGEFN to each pair of matching rows."
  (let ((result nil))
    (dolist (l left)
      (dolist (r right)
        (when (funcall matchfn (funcall lkeyfn l) (funcall rkeyfn r))
          (push (funcall mergefn l r) 'result))))
    result))


(cl-defmethod idf-collect ((df idf-op-join))
  "Build left and right input for DF then join them.

If the join condition is an equi-join, then use hash-join,
otherwise nested loop."
  (let* ((left-input (car (idf--df-inputs df)))
         (right-input (cadr (idf--df-inputs df)))
         (matchfn (idf-op-join-match-fn df))
         (mergefn (idf-op-join-merge-fn df))
         (lkey (idf-op-join-left-key-extractor df))
         (rkey (idf-op-join-right-key-extractor df)))
    ;; if equi-join?
    (if (equal (idf-op-join-match-fn df) 'equal)
        (pcase (idf-op-join-join-type df)
          ('inner
           (let ((right-ht (ht-create 'equal)))
             ;; build HT
             (--each (idf-collect right-input)
               (idf-ht-value-list-append right-ht (funcall rkey it) it))
             (idf--hash-join right-ht lkey (idf-collect left-input) mergefn nil)))
          ;; left-outer
          ('left-outer
           (let ((right-ht (ht-create 'equal)))
             ;; build HT
             (--each (idf-collect right-input)
               (idf-ht-value-list-append right-ht (funcall rkey it) it))
             (idf--hash-join right-ht lkey (idf-collect left-input) mergefn nil
                             :join-type 'left-outer
                             :right-attrs (idf--df-schema right-input))))
          ;; right-outer
          ('right-outer
           (let ((left-ht (ht-create 'equal)))
             ;; build HT
             (--each (idf-collect left-input)
               (idf-ht-value-list-append left-ht (funcall lkey it) it))
             (idf--hash-join left-ht rkey (idf-collect right-input) mergefn t
                             :join-type 'left-outer
                             :right-attrs (idf--df-schema left-input))))
          ;; full outer
          ('full-outer
           (let ((left-ht (ht-create 'equal))
                 (right-ht (ht-create 'equal)))
             ;; build HTs
             (--each (idf-collect left-input)
               (idf-ht-value-list-append left-ht (funcall lkey it) it))
             (--each (idf-collect right-input)
               (idf-ht-value-list-append right-ht (funcall rkey it) it))
             (idf--hash-join left-ht rkey (idf-collect right-input) mergefn t
                             :join-type 'left-outer
                             :left-attrs (idf--df-schema left-input)
                             :right-attrs (idf--df-schema right-input)
                             :right-ht right-ht
                             ;; :right-keyfn rkey
                             )))
           )
      ;; non-equi, use nested loop
      (let ((left-results (idf-collect left-input))
            (right-results (idf-collect right-input))) ;;TODO outer joins
        (idf--nested-loop-join
         left-results
         right-results
         lkey
         rkey
         matchfn
         mergefn)))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-op-join))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
  (setf (idf--df-setup-for-ivm df) t)
  (let ((matchfn (idf-op-join-match-fn df)))
    (if (equal matchfn 'equal)
        ;; hashjoin
        (progn
          (setf (idf-op-join-left-result df) (ht-create 'equal))
          (setf (idf-op-join-right-result df) (ht-create 'equal)))
      (setf (idf-op-join-left-result df) nil)
      (setf (idf-op-join-right-result df) nil))))

(cl-defmethod idf-materialize ((df idf-op-join))
  (unless (idf--df-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (setf (idf--df-result df) (idf-collect df)))

(cl-defmethod idf-apply-delta ((df idf-op-join) (deltas list))
  (unless (idf--df-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (let ((matchfn (idf-op-join-match-fn df))
        (mergefn (idf-op-join-merge-fn df))
        (left-result (idf-op-join-left-result df))
        (right-result (idf-op-join-right-result df))
        (lkey (idf-op-join-left-key-extractor df))
        (rkey (idf-op-join-right-key-extractor df))
        delta-ins
        delta-del)
    ;; DELTA: INSERTED: INS L JOIN R UNION L JOIN INS R UNION INS L JOIN INS R UNION DEL L JOIN DEL R
    ;;        DELETED: DEL L JOIN R UNION L JOIN DEL R
    (with-binary-delta deltas
                       (if (equal matchfn 'equal)
                           ;; ********************************************************************************
                           ;; hashjoin
                           (progn
                             ;; DELTA L JOIN R
                             (setq delta-ins (idf--hash-join right-result lkey l-ins mergefn t))
                             (setq delta-del (idf--hash-join right-result lkey l-del mergefn t))
                             ;; L JOIN DELTA R
                             (setq delta-ins (append delta-ins
                                                     (idf--hash-join left-result rkey r-ins mergefn nil)))
                             (setq delta-del (append delta-del
                                                     (idf--hash-join left-result rkey r-del mergefn nil)))
                             ;; DELTA L JOIN DELTA R
                             (setq delta-ins (append delta-ins
                                                     (idf--nested-loop-join l-ins r-ins lkey rkey matchfn mergefn)))
                             (setq delta-ins (append delta-ins
                                                     (idf--nested-loop-join l-del r-del lkey rkey matchfn mergefn))))
                         ;; ********************************************************************************
                         ;; nested loop
                         ;; DELTA L JOIN R
                         (setq delta-ins (idf--nested-loop-join l-ins right-result lkey rkey matchfn mergefn))
                         (setq delta-del (idf--nested-loop-join l-del right-result lkey rkey matchfn mergefn))
                         ;; L JOIN DELTA R
                         (setq delta-ins (append delta-ins (idf--nested-loop-join left-result r-ins lkey rkey matchfn mergefn)))
                         (setq delta-del (append delta-del (idf--nested-loop-join left-result r-del lkey rkey matchfn mergefn)))
                         ;; DELTA L JOIN DELTA R
                         (setq delta-ins (append delta-ins (idf--nested-loop-join l-ins r-ins lkey rkey matchfn mergefn)))
                         (setq delta-ins (append delta-ins (idf--nested-loop-join l-del r-del lkey rkey matchfn mergefn)))))
    (idf-delta-create
     :inserted delta-ins
     :deleted delta-del)))

;; ********************************************************************************
;; MV
(cl-defstruct (idf-mv
               (:include idf--df)
               (:constructor idf-mv--create))
  (mvtype 'hashtable :type symbol :documentation "Data structure to use to store the materialized view.

Should be either `hashtable' or sorted `list'")
  (cmpfn nil :type symbol :documentation "Use this function as the smaller-then function for sorted lists.

For hashtables this is the hash test (created with
`define-hash-table-test'")
  (keyfn nil :type function :documentation "For hashtables use this function to extract a key from a tuple.")
  (valuefn nil :type function :documentation "For hashtables use this function to extract the value from a tuple.")
  (store-as-symbol nil :type symbol :documentation "Store mv as this symbol."))

(cl-defmethod idf-collect ((df idf-mv))
(idf-collect (car (idf--df-inputs df))))

(cl-defmethod idf-prepare-for-maintenance ((df idf-mv))
  (dolist (i (idf--df-inputs df))
    (idf-prepare-for-maintenance i))
  (unless (idf--df-setup-for-ivm df)
    (setf (idf--df-setup-for-ivm df) t)
    (pcase (idf-mv-mvtype df)
      ('hashtable
       (setf (idf--df-result df)
             (ht-create (idf-mv-cmpfn df))))
      ('sortedlist
       (setf (idf--df-result df)
             (sorted-list-create nil (idf-mv-cmpfn df)))))))

(cl-defmethod idf-materialize ((df idf-mv))
  (unless (idf--df-setup-for-ivm df)
    (idf-prepare-for-maintenance df))
  (let ((inputds (car (idf--df-inputs df)))
        (storesym (idf-mv-store-as-symbol df)))
    (pcase (idf-mv-mvtype df)
      ('hashtable
       (let* ((input (idf-collect inputds))
             (keyfn (idf-mv-keyfn df))
             (valuefn (idf-mv-valuefn df))
             (ht (idf--df-result df)))
         (setf (idf--df-result df) ht)
         (dolist (i input)
           (ht-set ht (funcall keyfn i) (funcall valuefn i)))
         (when storesym
           (set storesym ht))))
      ('sortedlist
       (let ((input (idf-collect inputds)))
         (setf (idf--df-result df)
               (sorted-list-create input (idf-mv-cmpfn df)))
         (when storesym
           (set storesym (sorted-list-list (idf--df-result df)))))))))

(cl-defmethod idf-apply-delta ((df idf-mv) (delta list))
  "Apply DELTA to materialized view IDF-MV and return DELTA."
  (let  ((storesym (idf-mv-store-as-symbol df)))
    (pcase (idf-mv-mvtype df)
      ('hashtable
       (let ((ht (idf--df-result df))
             (keyfn (idf-mv-keyfn df))
             (valuefn (idf-mv-valuefn df)))
         (with-delta (car delta)
           (--each del (ht-remove ht (funcall keyfn it)))
           (--each ins (ht-set ht (funcall keyfn it) (funcall valuefn it))))
         (when storesym
           (set storesym ht))
         (car delta))) ;;FIXME hashtable behaves all unique so adapt the delta
      ('sortedlist
       (let ((sl (idf--df-result df)))
         (with-delta (car delta)
           (--each del (sorted-list-delete sl it))
           (--each ins (sorted-list-insert sl it)))
         (when storesym
           (set storesym (sorted-list-list sl)))
         (car delta))))))

(defun idf-mv-create-smallerfn (sortkeys &optional types)
  "Create a function comparing two plists on SORTKEYS.

Return t if the first one is smaller than the second one on
SORTKEYS. TYPES may be used in the future to determine the right
smaller-then function for each attribute."
  (ignore types)
  `(lambda (a b)
     (equal
      (--first it
               (--map (let ((av (plist-get a it)) (bv (plist-get b it)))
                        (cond
                         ;; equal -> nil
                         ((equal av bv) nil)
                         ;; smaller -> -1
                         ((or (and av (not bv)) (and av bv (< av bv)))
                          -1)
                         ;; larger 1
                         (t 1)))
                      ',sortkeys))
      -1)))

(defun idf-create-extractor (attrs)
  "Create a function that projects tuples on ATTRS."
  (if attrs
      (if (> (length attrs) 1)
          `(lambda (x) (--map (plist-get x it) ',attrs))
        `(lambda (x) (plist-get x ',(car attrs))))
    `(lambda (x) t)))

(defun idf-mv-get-result (mv)
  "Get the materialized result of a materialized view MV."
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
    (if (cl-loop for (key _) on l by #'cddr
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
the result dataframe can be provided." ;;TODO update doc
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

;;;###autoload
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

;;;###autoload
(defun idf-maintain-multiple (dfs updates &optional delta-cache)
  "Given list of dataframes DFS, apply UPDATES.

We incrementally maintain the results for all these dataframes.
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
;;;###autoload
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

;;;###autoload
(cl-defun idf-reduce (df &key reducefn groupfn groupattrs initval type resultschema)
  "Create dataframe DF that applies REDUCEFN to merge input into a single row.

If INITVAL is non-nil, then initialize the result to INITVAL. If
RESULTSCHEMA is provided then rename attributes accordingly. If
GROUPATTRS is non-nil, then group the rows of the dataframe on
these columns and apply the reducer to every group.
Alternatively, GROUPFN can be used to determine the key of
input (inputs with the same key are put into the same group."
  (idf--op-reduce-create
   :reduce-fn reducefn
   :group-fn (or groupfn (idf-create-extractor groupattrs))
   :init-val initval
   :type (if resultschema 'plist (or type 'list))
   :schema resultschema
   :inputs `(,df)))

;;;###autoload
(cl-defun idf-create-source (&key name content generatorfn updatefn schema)
  "Create a idf source dataframe named NAME.

A source dataframe is either created from some existing CONTENT (a list) or
using a function that returns the content (GENERATORFN). If a
source's content changes over time then a UPDATEFN should be
provided that returns a `idf-delta` which stores which elements
have been inserted and deleted from the source. Typically idf
dataframes are lists of lists where the inner lists are alists with
a fixed SCHEMA that can be provided as an input to this function."
  ;; (when (and (not content) (not generatorfn))
  ;;   (error "Sources are either created from literal content or from a generator functions"))
  (if (not generatorfn)
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

;;;###autoload
(cl-defun idf-project (df &key attrs resultschema)
  "Project a dataframe DF (which has to have a schema) on ATTRS.

if RESULTSCHEMA is provided, then rename attributes like this."
  ;;TODO check for schema
  (idf--op-map-create
   :schema (or resultschema attrs)
   :inputs `(,df)
   :map-fn (lambda (tuple)
             (-flatten
              (--map (list it (plist-get tuple it))
                     attrs)))))

;;;###autoload
(cl-defun idf-filter (df &key expr fn)
  "Filter the rows of DF.

If EXPR is provided then generate a function based on the EXPR.
Any symbol $NAME in EXPR will be replaced with the value of the
attribute NAME, e.g., `(< $A $B)` would filter out all rows where
the value of attribute A is smaller than the one of attribute B.
If FN is provided the evaluate FN on every input row and if it
returns nil, then filter out the row."
  (idf--op-filter-create
   :schema (idf--df-schema df)
   :inputs `(,df)
   :predicate-fn (if fn
                     fn
                   (idf-expr-to-lambda expr))))

;;;###autoload
(defun idf-union (leftdf rightdf &optional schema)
  "Union LEFTDF and RIGHTDF."
  (idf--op-union-create
   :schema (or schema (idf--df-schema leftdf))
   :type (idf--df-type leftdf)
   :inputs `(,leftdf ,rightdf)))

;;;###autoload
(cl-defun idf-unique (df &key schema keyextractor)
  "Remove duplicates from DF.

If KEYEXTRACTOR is used extract a key from tuples to determine
equality for duplicate elimination. If SCHEMA is provided then
rename the result attributes using SCHEMA."
  (idf--op-unique-create
   :type (idf--df-type df)
   :schema (or schema (idf--df-schema df))
   :inputs `(,df)
   :keyfn (or keyextractor 'identify)))

;;;###autoload
(cl-defun idf-aggregate (df &key group-by aggs schema)
  "Group DF on GROUP-BY attributes.

Then compute aggregation functions AGGS for each group.
Optionally, the attribute names for the result can be provided as
parameter SCHEMA. Essentially, this is a specifialized version of
`idf-reduce' which uses one of a fixed set of aggregation
functions. The implementation exploits the properties of the
aggregation functions to improve performance, e.g., for a `sum' we
can incrementally maintain the sum by updating the current result
as follows: inserted values are added to the current sum while
deleted values are subtracted from the current sum.

Currently, the following aggregates are supported:
- `sum'
- `avg'
- `min'
- `max'"
  (idf--op-aggregate-create
   :type 'plist
   :schema (or schema (idf--agg-create-schema group-by aggs))
   :group-bys group-by
   :agg-functions aggs
   :inputs `(,df)))

;;;###autoload
(cl-defun idf-equi-join (left-df right-df &key left-attrs right-attrs schema merge-fn join-type)
  "Join LEFT-DF with RIGHT-DF on equality.

Attributes LEFT-ATTRS and RIGHT-ATTRS and the attributes from
LEFT-DF (RIGHT_DF) we are joining on. Rename the result
attributes using SCHEMA (if provided). If MERGE-FN is provided
then this function (which has to take two arguments) will be
applied to every pair of matching elements to produce a result
element. The default function is to appedn the two input
elements (plists). If JOIN-TYPE is provided, then use this join
type (`inner', `left-outer', `right-outer', and `'full-outer'."
  (idf--op-join-create
   :type 'plist
   :schema (or schema (idf--join-create-schema left-df right-df))
   :match-fn 'equal
   :left-key-extractor (idf--generate-attr-key-extractor left-attrs)
   :right-key-extractor (idf--generate-attr-key-extractor right-attrs)
   :merge-fn (or merge-fn 'append) ;; todo rename
   :join-type (or join-type 'inner)
   :inputs `(,left-df ,right-df)))

(cl-defun idf-left-equi-join (left-df right-df &key left-attrs right-attrs schema merge-fn)
  "Left-outer join LEFT-DF with RIGHT-DF on equality.

Attributes LEFT-ATTRS and RIGHT-ATTRS and the attributes from
LEFT-DF (RIGHT_DF) we are joining on. Rename the result
attributes using SCHEMA (if provided). If MERGE-FN is provided
then this function (which has to take two arguments) will be
applied to every pair of matching elements to produce a result
element. The default function is to appedn the two input
elements (plists)."
  (idf-equi-join left-df right-df
                 :left-attrs left-attrs
                 :right-attrs right-attrs
                 :schema schema
                 :merge-fn merge-fn
                 :join-type 'left-outer))

(cl-defun idf-right-equi-join (left-df right-df &key left-attrs right-attrs schema merge-fn)
  "Left-outer join LEFT-DF with RIGHT-DF on equality.

Attributes LEFT-ATTRS and RIGHT-ATTRS and the attributes from
LEFT-DF (RIGHT_DF) we are joining on. Rename the result
attributes using SCHEMA (if provided). If MERGE-FN is provided
then this function (which has to take two arguments) will be
applied to every pair of matching elements to produce a result
element. The default function is to appedn the two input
elements (plists)."
  (idf-equi-join left-df right-df
                 :left-attrs left-attrs
                 :right-attrs right-attrs
                 :schema schema
                 :merge-fn merge-fn
                 :join-type 'right-outer))

(cl-defun idf-full-equi-join (left-df right-df &key left-attrs right-attrs schema merge-fn)
  "Left-outer join LEFT-DF with RIGHT-DF on equality.

Attributes LEFT-ATTRS and RIGHT-ATTRS and the attributes from
LEFT-DF (RIGHT_DF) we are joining on. Rename the result
attributes using SCHEMA (if provided). If MERGE-FN is provided
then this function (which has to take two arguments) will be
applied to every pair of matching elements to produce a result
element. The default function is to appedn the two input
elements (plists)."
  (idf-equi-join left-df right-df
                 :left-attrs left-attrs
                 :right-attrs right-attrs
                 :schema schema
                 :merge-fn merge-fn
                 :join-type 'full-outer))

;; ********************************************************************************
;; HELPER FUNCTIONS
(defun idf--generate-attr-key-extractor (attrs)
  "Generate a lambda that projects a plist on keys ATTRS."
  (let ((fetch-attrs (apply 'append (--map `((plist-get tup ,it)) attrs))))
    `(lambda (tup)
       (list ,@fetch-attrs))))

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
