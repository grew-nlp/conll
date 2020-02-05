open Conll_types

exception Conll_error of Yojson.Basic.t

module Sentence : sig
  val fr_clean_spaces: string -> string
end

module Id_with_proj: sig
  type t = Id.t * int option
end

module Id_with_proj_set : Set.S with type elt = Id_with_proj.t

module Mwe : sig
  type kind = Ne | Mwe

  type t = {
    mwepos: string option;
    kind: kind;
    label: string option;
    criterion: string option;
    first: Id_with_proj.t;
    items: Id_with_proj_set.t;
  }
end

module Conll : sig
  module Id: Conll_types.Id_type

  type line = {
    line_num: int;
    id: Id.t;
    form: string;
    lemma: string;
    upos: string;
    xpos: string;
    feats: (string * string) list;
    deps: (Id.t * string ) list;
    efs: (string * string) list;
  }

  val build_line:
    id:Id.t ->
    form: string ->
    ?lemma: string ->
    ?upos: string ->
    ?xpos: string ->
    ?feats: (string * string) list ->
    ?deps: (Id.t * string ) list ->
    unit ->
    line

  val root: line
  val compare: line -> line -> int

  type multiword = {
    mw_line_num: int option;
    first: int;
    last: int;
    fusion: string;
    mw_efs: (string * string) list;
  }

  type t = {
    file: string option;
    meta: string list;
    lines: line list;
    multiwords: multiword list;
    mwes: Mwe.t Int_map.t;
  }

  val void: t
  val is_void: t -> bool
  val from_string: string -> t
  val to_string: ?cupt:bool -> t -> string
  val to_dot: t -> string
  val save_dot: string -> t -> unit
  val load: ?tf_wf:bool -> string -> t
  val get_sentid_meta: t -> string option
  val get_sentid: t -> string option
  val set_sentid: string -> t -> t
  val set_label: Id.t -> string -> t -> t

  val ensure_sentid_in_meta: ?default:string -> t -> t

  val normalize_multiwords: t -> t

  val build_sentence: t -> string
  val get_sentence: t -> string option
  val html_sentence: ?highlight: int list -> t -> string

  val merge: string -> t -> t -> t
end

module Conll_corpus : sig
  type t = (string * Conll.t) array

  (* In the next 3 functions, if a log_file is given, data are loaded in a robust mode:
    no error is produced on Conll error but problems are reported in the given log_file.
    If the log_file already doesn't exist, it is created; else data are append to the file.
    *)
  val load: ?tf_wf:bool -> ?log_file:string -> string -> t
  val load_list: ?tf_wf: bool -> ?log_file:string -> string list -> t
  val from_lines: ?tf_wf: bool -> ?log_file:string -> ?basename: string -> (int * string) list -> t
  val save: string -> t -> unit
  val save_sub: string -> int -> int -> t -> unit
  val dump: t -> unit
  val token_size: t -> int

  (* [web_anno corpus base_output size]
    outputs a sequences of files of [size] sentences,
    prepared for input in webanno  *)
  val web_anno: t -> string -> int -> unit

  val get: string -> t -> Conll.t option
end

module Stat : sig
  type t

  type key = Upos | Xpos

  val build: key -> Conll_corpus.t -> t

  val dump: t -> unit

  (* build the table file. Args: corpus_id stat *)
  val to_html: string -> t -> string
end