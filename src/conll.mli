module Conll : sig

  type line = {
    line_num: int;
    id: int;
    form: string;
    lemma: string;
    upos: string;
    xpos: string;
    feats: (string * string) list;
    deps: (int * string ) list;
  }

  val root: line
  val compare: line -> line -> int

  type multiword = {
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

  (* val parse: string -> (int * string) list -> t *)
  val from_string: string -> t
  val to_string: t -> string
  val load: string -> t
  val get_sentid: t -> string option
end

module Conll_corpus : sig
  type t = (string * Conll.t) array
  val load: string -> t
  val load_list: string list -> t
  val save: string -> t -> unit
end
