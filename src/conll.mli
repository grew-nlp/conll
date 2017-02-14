module Sentence : sig
  val fr_clean_spaces: string -> string
end

module Conll : sig

  exception Error of string

  type line = {
    line_num: int;
    id: int;
    form: string;
    lemma: string;
    upos: string;
    xpos: string;
    feats: (string * string) list;
    deps: (int * string ) list;
    efs: (string * string) list;
  }

  val build_line:
    id:int -> 
    form: string -> 
    ?lemma: string ->
    ?upos: string ->
    ?xpos: string ->
    ?feats: (string * string) list ->
    ?deps: (int * string ) list ->
    unit ->
    line

  val root: line
  val compare: line -> line -> int

  type multiword = {
    mw_line_num: int;
    first: int;
    last: int;
    fusion: string;
  }

  type t = {
    file: string option;
    meta: string list;
    lines: line list;
    multiwords: multiword list;
  }

  val from_string: string -> t
  val to_string: t -> string
  val load: string -> t
  val get_sentid_meta: t -> string option
  val get_sentid: t -> string option

  val ensure_sentid_in_meta: t -> t 

  val build_sentence: t -> string
  val get_sentence: t -> string option
end

module Conll_corpus : sig
  type t = (string * Conll.t) array
  val load: string -> t
  val load_list: string list -> t
  val save: string -> t -> unit
  val dump: t -> unit
end
