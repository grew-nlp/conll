(lang dune 3.5)

(name conll)

(version 1.19.2)

(generate_opam_files true)

(source
 (github grew-nlp/conll))

(authors "Bruno Guillaume")

(maintainers "Bruno.Guillaume@inria.fr")

(license CECILL-2.1)

(package
 (name conll)
 (synopsis "Manipulation of CoNLL files")
 (description "Mainly developed to be used by Grew")
 (depends ocaml dune containers yojson)
 (tags
  (nlp))
 (conflicts libcaml-conll)
)
