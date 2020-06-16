open Printf

exception Conllx_error of Yojson.Basic.t

(* ==================================================================================================== *)
module Error = struct
  exception Skip
  let robust = ref false

  (* ---------------------------------------------------------------------------------------------------- *)
  let build_json ?file ?sent_id ?line_num ?fct ?data ?prev message =
    let opt_list = [
      Some ("message", `String message);
      (CCOpt.map (fun x -> ("file", `String x)) file);
      (CCOpt.map (fun x -> ("sent_id", `String x)) sent_id);
      (CCOpt.map (fun x -> ("line", `Int x)) line_num);
      (CCOpt.map (fun x -> ("function", `String x)) fct);
      (CCOpt.map (fun x -> ("data", x)) data);
    ] in
    let prev_list = match prev with
      | None -> [("library", `String "Conllx")]
      | Some (`Assoc l) -> l
      | Some json -> ["ill_formed_error", json] in
    `Assoc ((CCList.filter_map (fun x-> x) opt_list) @ prev_list)

  (* ---------------------------------------------------------------------------------------------------- *)
  let warning_ ?file ?sent_id ?line_num ?fct ?data ?prev message =
    Printf.eprintf "%s\n" (Yojson.Basic.pretty_to_string (build_json ?file ?sent_id ?line_num ?fct ?data ?prev message))

  (* ---------------------------------------------------------------------------------------------------- *)
  let warning ?file ?sent_id ?line_num ?fct ?data ?prev = Printf.ksprintf (warning_ ?file ?sent_id ?line_num ?fct ?data ?prev)

  (* ---------------------------------------------------------------------------------------------------- *)
  let error_ ?file ?sent_id ?line_num ?fct ?data ?prev message =
    let json = build_json ?file ?sent_id ?line_num ?fct ?data ?prev message in
    if !robust
    then (Printf.eprintf "%s\n" (Yojson.Basic.pretty_to_string json); raise Skip)
    else raise (Conllx_error json)

  (* ---------------------------------------------------------------------------------------------------- *)
  let error ?file ?sent_id ?line_num ?fct ?data ?prev = Printf.ksprintf (error_ ?file ?sent_id ?line_num ?fct ?data ?prev)

  (* ---------------------------------------------------------------------------------------------------- *)
  let reraise ?file ?sent_id ?line_num ?fct ?data ?prev json =
    let new_json = match json with
      | `Assoc list -> `Assoc (
          list
          |> (fun l -> match file with None -> l | Some x -> ("file", `String x) :: (List.remove_assoc "file" l))
          |> (fun l -> match sent_id with None -> l | Some x -> ("sent_id", `String x) :: (List.remove_assoc "sent_id" l))
          |> (fun l -> match line_num with None -> l | Some x -> ("line_num", `String x) :: (List.remove_assoc "line_num" l))
          |> (fun l -> match fct with None -> l | Some x -> ("fct", `String x) :: (List.remove_assoc "fct" l))
          |> (fun l -> match data with None -> l | Some x -> ("data", x) :: (List.remove_assoc "data" l))
          |> (fun l -> match prev with None -> l | Some x -> ("prev", `String x) :: (List.remove_assoc "prev" l))
        )
      | _ -> error "Bug in json error structure" in
    raise (Conllx_error new_json)
end

(* ==================================================================================================== *)
module String_map = CCMap.Make (String)
module Int_map = CCMap.Make (Int)

(* ==================================================================================================== *)
module Misc = struct

  (* ---------------------------------------------------------------------------------------------------- *)

  (* "f=v|g=w" --> [("f", "v"); ("g", "w")] *)
  let parse_features ?file ?sent_id ?line_num s =
    match s with
    | "_" -> []
    | _ ->
      List.map
        (fun feat ->
           match Str.bounded_full_split (Str.regexp "=") feat 2 with
           | [Str.Text f; Str.Delim "="; Str.Text v] -> (f, v)
           | [Str.Text f; Str.Delim "="] -> (f, "")
           (* accept features without values. This happens in MISC column in a few UD corpora and in FEATS column in PARSEME-TR@1.1 *)
           | [Str.Text f] -> (f,"__NOVALUE__")
           | _ -> Error.error ?file ?sent_id ?line_num "Unknown feat %s" feat
        ) (Str.split (Str.regexp "|") s)

  (* ---------------------------------------------------------------------------------------------------- *)
  let read_lines name =
    try
      let ic = open_in name in
      let cpt = ref 0 in
      let try_read () =
        try incr cpt; Some (!cpt, input_line ic) with End_of_file -> None in
      let rec loop acc = match try_read () with
        | Some s -> loop (s :: acc)
        | None -> close_in ic; List.rev acc in
      loop []
    with Sys_error msg -> Error.error "%s" msg

  (* ---------------------------------------------------------------------------------------------------- *)
  let read_stdin () =
    let cpt = ref 0 in
    let res = ref [] in
    try
      while true do
        incr cpt;
        res := (!cpt, input_line stdin) :: !res
      done;
      assert false
    with End_of_file -> List.rev !res

end

(* ==================================================================================================== *)
module Column = struct
  type t = ID | FORM | LEMMA | UPOS | XPOS | FEATS | HEAD | DEPREL | DEPS | MISC
         | PARSEME_MWE
         | SEMCOR_NOUN
         | ORFEO_START
         | ORFEO_STOP
         | ORFEO_SPEAKER

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string = function
    | ID -> "ID"
    | FORM -> "FORM"
    | LEMMA -> "LEMMA"
    | UPOS -> "UPOS"
    | XPOS -> "XPOS"
    | FEATS -> "FEATS"
    | HEAD -> "HEAD"
    | DEPREL -> "DEPREL"
    | DEPS -> "DEPS"
    | MISC -> "MISC"
    | PARSEME_MWE -> "PARSEME:MWE"
    | SEMCOR_NOUN -> "SEMCOR:NOUN"
    | ORFEO_START -> "ORFEO:START"
    | ORFEO_STOP -> "ORFEO:STOP"
    | ORFEO_SPEAKER -> "ORFEO:SPEAKER"

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string ?file ?line_num = function
    | "ID" -> ID
    | "FORM" -> FORM
    | "LEMMA" -> LEMMA
    | "UPOS" -> UPOS
    | "XPOS" -> XPOS
    | "FEATS" -> FEATS
    | "HEAD" -> HEAD
    | "DEPREL" -> DEPREL
    | "DEPS" -> DEPS
    | "MISC" -> MISC
    | "PARSEME:MWE" -> PARSEME_MWE
    | "SEMCOR:NOUN" -> SEMCOR_NOUN
    | "ORFEO:START" -> ORFEO_START
    | "ORFEO:STOP" -> ORFEO_STOP
    | "ORFEO:SPEAKER" -> ORFEO_SPEAKER
    | x -> Error.error ?file ?line_num "Unknown Column %s" x
end

(* ==================================================================================================== *)
module Conllx_columns = struct

  type t = Column.t list

  (* ---------------------------------------------------------------------------------------------------- *)
  let default = [Column.ID; FORM; LEMMA; UPOS; XPOS; FEATS; HEAD; DEPREL; DEPS; MISC]
  let cupt = [Column.ID; FORM; LEMMA; UPOS; XPOS; FEATS; HEAD; DEPREL; DEPS; MISC; PARSEME_MWE]
  let orfeo = [Column.ID; FORM; LEMMA; UPOS; XPOS; FEATS; HEAD; DEPREL; DEPS; MISC; ORFEO_START; ORFEO_STOP; ORFEO_SPEAKER]

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string t =
    sprintf "# global.columns = %s"
      (String.concat " " (List.map Column.to_string t))

  (* ---------------------------------------------------------------------------------------------------- *)
  let build s = List.map Column.of_string (Str.split (Str.regexp " ") s)

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string ?file ?line_num t =
    match Str.split (Str.regexp " ") t with
    | "#" :: "global.columns" :: "=" :: l -> Some (List.map (Column.of_string ?file ?line_num) l)
    | _ -> None
end

(* ==================================================================================================== *)
module Conllx_config = struct
  type t = {
    name: string;
    core: string;                     (* name of the required part (no join symbol) *)
    extensions: (string * char) list; (* (field_name, join symbol) *)                  (* UD, SUD *)
    prefixes: (string * char) list;   (* (kind_value, prefix) *)                       (* Seq *)
    feats: string list;               (* feature values in FEATS column *)
    deps: (string * char) option;   (* edge feature value name for DEPS column, prefix for compact notation *)    (* EUD *)
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let default = {
    name = "default";
    core = "rel";
    extensions = [];
    prefixes = [];
    feats = [];
    deps = None;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let ud_features = [
    (* UD morphology *)
    "Abbr"; "AbsErgDatNumber"; "AbsErgDatPerson"; "AbsErgDatPolite"; "AdpType"; "AdvType"; "Animacy";
    "Aspect"; "Case"; "Clusivity"; "ConjType"; "Definite"; "Degree"; "Echo"; "ErgDatGender"; "Evident";
    "Foreign"; "Gender"; "Hyph"; "Mood"; "NameType"; "NounClass"; "NounType"; "NumForm"; "NumType";
    "NumValue"; "Number"; "PartType"; "Person"; "Polarity"; "Polite"; "Poss"; "PossGender"; "PossNumber";
    "PossPerson"; "PossedNumber"; "Prefix"; "PrepCase"; "PronType"; "PunctSide"; "PunctType"; "Reflex";
    "Style"; "Subcat"; "Tense"; "Typo"; "VerbForm"; "VerbType"; "Voice";
    "Number[psor]";
    (* SUD features *)
    "Shared"; "Deixis"; "DeixisRef"; "FocusType"; "AdjType";
  ]

  (* ---------------------------------------------------------------------------------------------------- *)
  let ud = { (* covers also eud *)
    name="ud";
    core = "1";
    extensions = [ ("2",':')];
    prefixes = [];
    feats = ud_features;
    deps = Some ("enhanced", 'E');
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let future_ud = { (* covers also eud *)
    name="ud";
    core = "rel";
    extensions = [ ("subrel",':')];
    prefixes = [];
    feats = ud_features;
    deps = Some ("enhanced", 'E');
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let sud = {
    name="sud";
    core = "1";
    extensions = [ ("2",':'); ("deep", '@') ];
    prefixes = [];
    feats = ud_features;
    deps = None;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let future_sud = {
    name="sud";
    core = "rel";
    extensions = [ ("subrel",':'); ("deep", '@') ];
    prefixes = [];
    feats = ud_features;
    deps = None;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let sequoia = {
    name="sequoia";
    core = "1";
    extensions = [ ("2",':') ];
    prefixes = [ ( "deep", 'D'); ("surf", 'S') ];
    feats = [
      "agent"; "cltype"; "component"; "def"; "diat"; "dl"; "dm"; "g";
      "intrinsimp"; "m"; "mwehead"; "mwelemma"; "n"; "p"; "s"; "se"; "t"; "void"
    ];
    deps = None;
  }

  let build = function
    | "sequoia" -> sequoia
    | "ud" -> ud
    | "sud" -> sud
    | "default" | "orfeo" -> default
    | s -> Error.error "Unknown config `%s` (available values are: `default`, `ud`, `sud`, `sequoia`, `orfeo`)" s

  let get_name t = t.name
end


(* ==================================================================================================== *)
module Id = struct
  type t =
    | Simple of int
    | Mwt of int * int   (* (3,4) --> "3-4" *)
    | Empty of int * int (* (8,2) --> "8.2" *)
    | Raw of string (* when built from json *)

  (* ---------------------------------------------------------------------------------------------------- *)
  let base = function Simple i | Mwt (i,_) | Empty (i,_) -> i | Raw _ -> failwith "Raw id"

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string = function
    | Simple id -> sprintf "%d" id
    | Mwt (init,final) -> sprintf "%d-%d" init final
    | Empty (base,sub) -> sprintf "%d.%d" base sub
    | Raw s -> s

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare ?file ?sent_id t1 t2 =
    match Stdlib.compare (base t1) (base t2) with
    | 0 ->
      begin
        match (t1, t2) with
        | (Mwt _, Simple _) -> -1 | (Simple _, Mwt _) -> 1
        | (Mwt _, Empty _) -> -1 | (Empty _, Mwt _) -> 1
        | (Simple _, Empty _) -> -1 | (Empty _, Simple _) -> 1
        | (Simple _, Simple _) -> 0
        | (Empty (_,sub1), Empty (_,sub2)) -> Stdlib.compare sub1 sub2
        | _ -> Error.error ?file ?sent_id "Invalid arg in Id.compare <%s> <%s>" (to_string t1) (to_string t2)
      end
    | n -> n

  (* ---------------------------------------------------------------------------------------------------- *)

  (* rebuild ids list to follow the given order (taking into account empty nodes) *)
  let normalise_list is_empty id_list =
    let rec loop = function
      | (_,[]) -> []
      | (Simple pos, head::tail) when is_empty head -> let new_id = Empty (pos, 1) in (head,new_id) :: (loop (new_id,tail))
      | (Empty (pos,i), head::tail) when is_empty head -> let new_id = Empty (pos,i+1) in (head,new_id) :: (loop (new_id,tail))
      | (id, head::tail) -> let new_id = Simple ((base id)+1) in (head,new_id) :: (loop (new_id,tail))
    in
    loop (Simple (-1), id_list)

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string  ?file ?sent_id ?line_num s =
    try
      match Str.bounded_full_split (Str.regexp "[.-]") s 2 with
      | [Str.Text string_id] -> Simple (int_of_string string_id)
      | [Str.Text string_init; Str.Delim "-"; Str.Text string_final] ->
        Mwt (int_of_string string_init,int_of_string string_final)
      | [Str.Text string_base; Str.Delim "."; Str.Text string_sub] ->
        Empty (int_of_string string_base,int_of_string string_sub)
      | _ -> Error.error  ?file ?sent_id ?line_num "Cannot parse id %s" s
    with Failure _ -> Error.error  ?file ?sent_id ?line_num "Cannot parse id %s" s
end

(* ==================================================================================================== *)
module Id_set = Set.Make (struct type t = Id.t let compare = Stdlib.compare end)

(* ==================================================================================================== *)
module Feat = struct

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare (f1,_) (f2,_) = Stdlib.compare (CCString.lowercase_ascii f1) (CCString.lowercase_ascii f2)

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string = function
    | [] -> "_"
    | l -> String.concat "|" (List.map (fun (f,v) -> f^"="^v) l)

  (* ---------------------------------------------------------------------------------------------------- *)
  let string_feats ~config misc feats =
    let filtered_feats = List.filter
        (function
          | ("lemma",_) | ("upos",_) | ("xpos",_) -> false
          | (s,_) when s.[0] = '_' -> false
          | _ when config.Conllx_config.feats = [] -> not misc
          | (f,_) when List.mem f config.feats -> not misc
          | _ -> misc
        ) feats in
    to_string filtered_feats
end

(* ==================================================================================================== *)
module Node = struct

  type t = {
    id: Id.t;
    form: string;
    feats: (string * string) list;
    wordform: string option;
    textform: string option;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let conll_root = { id = Id.Simple 0; form="__ROOT__"; feats=[]; wordform=None; textform=None;}

  (* ---------------------------------------------------------------------------------------------------- *)
  let is_conll_root t = t.form = "__ROOT__"

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare ?file ?sent_id n1 n2 = Id.compare ?file ?sent_id n1.id n2.id

  (* ---------------------------------------------------------------------------------------------------- *)
  let id_map mapping t =
    match List.assoc_opt t.id mapping with
    | Some new_id -> {t with id = new_id}
    | None -> t

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_item_list ?file ?sent_id ?line_num ~columns item_list =
    try
      let (id_opt, form, feats) =
        List.fold_left2
          (fun (acc_id_opt, acc_form, acc_feats) col item ->
             match (col, acc_id_opt) with
             | (Column.ID, None) -> (Some (Id.of_string  ?file ?sent_id ?line_num item), acc_form, acc_feats)
             | (Column.ID, Some id) -> Error.error ?file ?sent_id ?line_num "Duplicate id `%s`" (Id.to_string id)
             | (Column.FORM, _) ->
               (acc_id_opt, item, acc_feats)
             | (Column.LEMMA, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> ("lemma", item) :: acc_feats)
             | (Column.UPOS, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> ("upos", item) :: acc_feats)
             | (Column.XPOS, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> ("xpos", item) :: acc_feats)
             | (Column.FEATS,_) -> (acc_id_opt, acc_form, (Misc.parse_features ?file ?sent_id ?line_num item) @ acc_feats)
             | (Column.MISC,_) -> (acc_id_opt, acc_form, (Misc.parse_features ?file ?sent_id ?line_num item) @ acc_feats)
             | (Column.ORFEO_START, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> ("_start", item) :: acc_feats)
             | (Column.ORFEO_STOP, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> ("_stop", item) :: acc_feats)
             | (Column.ORFEO_SPEAKER, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> ("_speaker", item) :: acc_feats)
             | _ -> (acc_id_opt, acc_form, acc_feats)
          ) (None, "" ,[]) columns item_list in
      match id_opt with
      | None -> Error.error ?file ?sent_id ?line_num "No id"
      | Some id -> { id; form; feats = List.sort Feat.compare feats; wordform=None; textform=None; }
    with Invalid_argument _ ->
      Error.error ?file ?sent_id ?line_num
        "Wrong number of fields: %d instead of %d expected"
        (List.length item_list) (List.length columns)

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_json t = `Assoc (
      CCList.filter_map CCFun.id
        (
          (Some ("id", `String (Id.to_string t.id)))
          :: (Some ("form", `String t.form))
          :: (CCOpt.map (fun v -> ("textform", `String v)) t.textform)
          :: (CCOpt.map (fun v -> ("wordform", `String v)) t.wordform)
          :: (List.map (fun (f,v) -> Some (f, `String v)) t.feats)
        ))

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_json (json: Yojson.Basic.t) =
    let open Yojson.Basic.Util in
    try
      let feats =
        List.sort Feat.compare
          (CCList.filter_map
             (function
               | ("id",_) | ("form",_) |  ("wordform",_) |  ("textform",_) -> None
               | (k,v) -> Some (k, v |> to_string)
             ) (json |> to_assoc)
          )  in
      let id = Id.Raw (json |> member "id" |> to_string) in
      let form_opt = try Some (json |> member "form" |> to_string) with Type_error _ -> None in
      let form = match (id, form_opt) with
        | (_, Some f) -> f
        | (Simple 0, None) -> "__ROOT__"
        | (_, None) -> Error.error ~fct:"Node.of_json" ~data:json "missing form" in
      {
        id;
        form;
        feats;
        wordform = (try Some (json |> member "wordform" |> to_string) with Type_error _ -> None);
        textform = (try Some (json |> member "textform" |> to_string) with Type_error _ -> None);
      }
    with Type_error _ -> Error.error ~fct:"Node.of_json" ~data:json "illformed json"

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_conll ~config ~columns head deprel deps parseme_mwe t =
    String.concat "\t"
      (List.map (function
           | Column.ID -> Id.to_string t.id
           | Column.FORM -> t.form
           | Column.LEMMA -> (match List.assoc_opt "lemma" t.feats with Some l -> l | None -> "_")
           | Column.UPOS -> (match List.assoc_opt "upos" t.feats with Some l -> l | None -> "_")
           | Column.XPOS -> (match List.assoc_opt "xpos" t.feats with Some l -> l | None -> "_")
           | Column.FEATS -> Feat.string_feats ~config false t.feats
           | Column.HEAD -> head
           | Column.DEPREL -> deprel
           | Column.DEPS -> deps
           | Column.MISC -> Feat.string_feats ~config true t.feats
           | Column.PARSEME_MWE -> parseme_mwe
           | Column.ORFEO_START -> (match List.assoc_opt "_start" t.feats with Some l -> l | None -> "_")
           | Column.ORFEO_STOP -> (match List.assoc_opt "_stop" t.feats with Some l -> l | None -> "_")
           | Column.ORFEO_SPEAKER -> (match List.assoc_opt "_speaker" t.feats with Some l -> l | None -> "_")
           | _ -> "_"
         ) columns)

  (* ---------------------------------------------------------------------------------------------------- *)
  type mwt_misc = ((int * int) * (string * string) list) list

  (* ---------------------------------------------------------------------------------------------------- *)
  let mwt_misc_to_string (mwt_misc: mwt_misc) =
    String.concat "||"
      (List.map
         (fun ((i,f),feats) ->
            sprintf "%d::%d::%s" i f (Feat.to_string feats)
         ) mwt_misc
      )

  (* ---------------------------------------------------------------------------------------------------- *)
  let mwt_misc_of_string s : mwt_misc =
    List.map
      (fun item ->
         match Str.split (Str.regexp_string "::") item with
         | [si; sj; feats] -> ((int_of_string si, int_of_string sj), Misc.parse_features feats)
         | _ -> Error.error ~fct: "mwt_misc_of_string" "Cannot parse `%s`" s
      )  (Str.split (Str.regexp_string "||") s)

  (* ---------------------------------------------------------------------------------------------------- *)
  let escape_form = function "_" -> "UNDERSCORE" | x -> x

  (* ---------------------------------------------------------------------------------------------------- *)
  let unescape_form = function "UNDERSCORE" -> "_" | x -> x

  (* ---------------------------------------------------------------------------------------------------- *)
  let textform_up node_list =
    let mwt_misc = ref [] in
    let rec loop to_underscore = function
      | [] -> []
      | ({ form="__ROOT__" } as node) :: tail -> node :: (loop to_underscore tail)
      | ({ id=Id.Empty _; _ } as node) :: tail -> { node with textform = Some "_"} :: (loop to_underscore tail)
      | { id=Id.Mwt (init,final); form; feats; _} :: next :: tail ->
        (match feats with [] -> () | l -> mwt_misc := ((init,final),l) :: !mwt_misc);
        let new_to_underscore = (CCList.range (init+1) final) @ to_underscore in
        {next with textform = Some (escape_form form)} :: (loop new_to_underscore tail)
      | ({ id=Id.Simple i; _ } as node) :: tail when List.mem i to_underscore ->
        { node with textform = Some "_"} :: (loop (CCList.remove_one ~eq:(=) i to_underscore) tail)
      | node :: tail ->
        { node with textform = Some (escape_form node.form) } :: (loop to_underscore tail) in
    let new_node_list = loop [] node_list in
    (new_node_list, mwt_misc_to_string !mwt_misc)

  (* ---------------------------------------------------------------------------------------------------- *)
  let textform_down mwt_misc_string node_list =
    let mwt_misc = mwt_misc_of_string mwt_misc_string in
    let (in_span, new_node_list) =
      CCList.fold_map (
        fun acc node ->
          match (node.id, node.textform) with
          | (Id.Empty _, Some "_") -> (acc, {node with textform = None})
          | (Id.Simple i, Some "_") -> (i::acc, {node with textform = None})
          | _ -> (acc, {node with textform = None})
      ) [] node_list in
    let decr_in_span = List.sort (fun x y -> - (Stdlib.compare x y)) in_span in

    let rec loop = function
      | (None, []) -> []
      | (None, h::tail) -> loop (Some (h,h), tail)
      | (Some (i,f), []) -> [(i-1,f)]
      | (Some (i,f), h::tail) when h = i-1 -> loop (Some (h,f), tail)
      | (Some (i,f), l) -> (i-1,f)::(loop (None, l))
    in

    let find_original_textform i =
      match List.find_opt (fun node -> node.id = Id.Simple i) node_list with
      | Some node ->
        begin
          match node.textform with
          | Some tf -> unescape_form tf
          | None -> Error.error ~fct:"find_original_textform" "No `wordform` feature"
        end
      | None -> Error.error ~fct:"find_original_textform" "Cannot find node `%d`" i in

    let mwts =
      List.map
        (fun (i,j) ->
           match List.assoc_opt (i,j) mwt_misc with
           | None -> { id=Id.Mwt (i,j); form=unescape_form (find_original_textform i); feats=[]; wordform=None; textform=None }
           | Some feats -> { id=Id.Mwt (i,j); form=unescape_form (find_original_textform i); feats; wordform=None; textform=None }
        ) (loop (None, decr_in_span)) in

    List.sort compare (mwts @ new_node_list)

  (* ---------------------------------------------------------------------------------------------------- *)
  let wordform_up node_list =
    List.map
      (fun node ->
         match (node.id, List.assoc_opt "wordform" node.feats) with
         | _ when node.form = "__ROOT__" -> node
         | (_, Some wf) -> { node with wordform = Some wf; feats = List.remove_assoc "wordform" node.feats }
         | (Empty _, _) -> { node with wordform = Some "_" }
         | (_, None) -> { node with wordform = Some (escape_form node.form) }
      ) node_list

  (* ---------------------------------------------------------------------------------------------------- *)
  let wordform_down node_list =
    List.map
      (fun node ->
         match node.wordform with
         | Some "_" -> { node with wordform = None }
         | Some wf when (unescape_form wf) <> node.form ->
           { node with wordform = None; feats = List.sort Feat.compare (("wordform", wf) :: node.feats) }
         | Some _ -> { node with wordform = None }
         | None -> { node with wordform = None }
      ) node_list

  (* ---------------------------------------------------------------------------------------------------- *)
  let is_empty t = t.wordform = Some "_" && t.textform = Some "_"
end

(* ==================================================================================================== *)
module Conllx_label = struct

  type t = string String_map.t

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string_long t =
    String.concat ","
      (String_map.fold (fun k v acc -> (sprintf "%s=%s" k v) :: acc) t [])

  (* ---------------------------------------------------------------------------------------------------- *)
  exception Long
  let to_string ~config t =
    try
      match String_map.find_opt config.Conllx_config.core t with
      | None -> raise Long (* no core relation *)
      | Some rel ->
        let t = String_map.remove config.core t in

        let (t,pref_deps) =
          match config.deps with
          | Some (deps_feat_name, pref) ->
            begin
              match String_map.find_opt deps_feat_name t with
              | Some "yes" -> (String_map.remove deps_feat_name t, Some pref)
              | Some _ -> raise Long (* unexpected value for deps_feat_name *)
              | _ -> (t,None)
            end
          | None -> (t,None) in

        let pref_kind = match String_map.find_opt "kind" t with
          | None -> None
          | Some k -> match List.assoc_opt k config.prefixes with
            | None -> raise Long (* unknown "kind" value *)
            | Some p -> Some p in
        let t = String_map.remove "kind" t in

        let prefix_string = match (pref_deps, pref_kind) with
          | (Some _, Some _) -> Error.error ~fct:"Conllx.to_string" "BUG: Prefix confict, please report input=`%s`" (to_string_long t)
          | (Some c, None) | (None, Some c) -> sprintf "%c:" c
          | (None, None) -> "" in

        let (remaining, extensions_string) = List.fold_left
            (fun (acc_rem, acc_ext_string) (name, sym) ->
               match String_map.find_opt name t with
               | Some v -> (String_map.remove name acc_rem, sprintf "%s%c%s" acc_ext_string sym v)
               | None -> (acc_rem, acc_ext_string)
            ) (t,"") config.extensions in
        if String_map.is_empty remaining
        then Ok (prefix_string ^ rel ^ extensions_string)
        else raise Long (* there are more feature that cannot be encoded *)
    with Long -> Error (to_string_long t)

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string_robust ~config t = match to_string ~config t with Ok s | Error s -> s

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string ~config s =
    let (pref_feat_opt, pos) =
      if String.length s > 1 && s.[1] = ':'
      then
        match config.Conllx_config.deps with
        | Some (efn, pref_deps) when pref_deps = s.[0] -> (Some (efn, "yes"), 2)
        | _ ->
          match List.find_opt (fun (_,sym) -> sym = s.[0]) config.prefixes with
          | Some (v,_) -> (Some ("kind", v), 2)
          | None -> (None, 0)
      else (None, 0) in

    let rec loop feat p = function
      | [] -> String_map.singleton feat (CCString.drop p s)
      | (next,sym) :: tail ->
        match String.index_from_opt s p sym with
        | None -> loop feat p tail
        | Some new_pos -> String_map.add feat (CCString.Sub.copy (CCString.Sub.make s p (new_pos-p))) (loop next (new_pos+1) tail) in

    loop config.core pos config.extensions
    |> (fun x -> match pref_feat_opt with Some (f,v) -> String_map.add f v x | None -> x)

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string_deps ?file ?sent_id ?line_num ~config s =
    let pre_label = of_string ~config s in
    match config.deps with
    | Some (deps_feat_name,_) -> String_map.add deps_feat_name "yes" pre_label
    | None -> Error.error ?file ?sent_id ?line_num "No secondary edges expected with config: %s" config.name

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_json t = `Assoc (List.map (fun (x,y) -> (x,`String y)) (String_map.to_list t))

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_json js =
    let open Yojson.Basic.Util in
    js
    |> to_assoc
    |> (List.map (fun (k,v) -> k, to_string v))
    |> String_map.of_list

  (* let _ =
     printf "comp:aux@tense\n";
     let l = of_string "comp:aux@tense" in
     String_map.iter (fun f v -> printf "### %s = %s\n" f v) l;
     printf "==> %s\n" (to_string l)

     let _ =
     printf "D:subj:obj\n";
     let l = of_string ~config:Conllx_config.sequoia "D:subj:obj" in
     String_map.iter (fun f v -> printf "### %s = %s\n" f v) l;
     printf "==> %s\n" (to_string ~config:Conllx_config.sequoia l) *)
end

(* ==================================================================================================== *)
module Edge = struct
  type t = {
    src: Id.t;
    label: Conllx_label.t;
    tar: Id.t;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let id_map mapping t =
    let new_src = match List.assoc_opt t.src mapping with Some new_id -> new_id | None -> t.src in
    let new_tar = match List.assoc_opt t.tar mapping with Some new_id -> new_id | None -> t.tar in
    {t with src=new_src; tar=new_tar }

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare ~config e1 e2 =
    match Id.compare e1.src e2.src with
    | 0 -> Stdlib.compare (Conllx_label.to_string ~config e1.label) (Conllx_label.to_string ~config e2.label)
    | n -> n
  let is_tar id edge = edge.tar = id

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_item_list ?file ?sent_id ?line_num ~config ~columns tar item_list =
    try
      let (src_opt, label_opt, sec_opt) =
        List.fold_left2
          (fun (src_acc, label_acc, sec_acc) col item ->
             match (item, col, src_acc, label_acc, sec_acc) with
             | ("_",_,_,_,_) -> (src_acc, label_acc, sec_acc)
             | (_,Column.HEAD, None, _, _) -> (Some item, label_acc, sec_acc)
             | (_,Column.DEPREL, _, None, _) -> (src_acc, Some item, sec_acc)
             | (_,Column.DEPS, _, _, None) -> (src_acc, label_acc, Some item)
             | _ -> (src_acc, label_acc, sec_acc)
          ) (None, None, None) columns item_list in

      let head_dep_edges =
        match (src_opt, label_opt) with

        (* PARSEME-SL@1.1 has the pair ("-", "-") as HEAD/DEPREL *)
        | (Some "-", Some "-") -> []

        (* PARSEME-IT@1.1 and PARSEME-IT@1.1 has the pair ("0", "_") as HEAD/DEPREL *)
        | (Some "0", None) -> [{ src=Id.of_string  ?file ?sent_id ?line_num "0"; label=Conllx_label.of_string ~config "root"; tar }]

        | (Some srcs, Some labels) ->
          let src_id_list = List.map (Id.of_string  ?file ?sent_id ?line_num) (Str.split (Str.regexp "|") srcs) in
          let label_list = List.map (Conllx_label.of_string ~config) (Str.split (Str.regexp "|") labels) in
          begin
            try List.map2 (fun src label -> { src; label; tar}) src_id_list label_list
            with Invalid_argument _ -> Error.error ?file ?sent_id ?line_num "different number of items in HEAD/DEPREL spec"
          end
        | (None, None) -> []
        | _ -> Error.error ?file ?sent_id ?line_num "Invalid HEAD/DEPREL spec" in

      match sec_opt with
      | None -> head_dep_edges
      | Some deps ->
        List.fold_left
          (fun acc sec ->
             match Str.bounded_split (Str.regexp ":") sec 2 with
             | [src_string;label] -> { src=Id.of_string  ?file ?sent_id ?line_num src_string; label = Conllx_label.of_string_deps ?file ?sent_id ?line_num ~config label; tar} :: acc
             | _ -> Error.error ?file ?sent_id ?line_num "Cannot parse secondary edges `%s`" sec
          ) head_dep_edges (Str.split (Str.regexp "|") deps)

    with Invalid_argument _ ->
      Error.error ?file ?sent_id ?line_num
        "Wrong number of fields: %d instead of %d expected"
        (List.length item_list) (List.length columns)

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_json t = `Assoc [
      ("src", `String (Id.to_string t.src));
      ("label", Conllx_label.to_json t.label);
      ("tar", `String (Id.to_string t.tar));
    ]

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_json json =
    let open Yojson.Basic.Util in
    try
      {
        src = Id.Raw (json |> member "src" |> to_string);
        label = json |> member "label" |> Conllx_label.of_json;
        tar = Id.Raw (json |> member "tar" |> to_string);
      }
    with Type_error _ -> Error.error ~fct:"Edge.of_json" ~data:json "illformed json"


  (* ---------------------------------------------------------------------------------------------------- *)

  (* [split] an edge list in two lists: basic edges (HEAD/DEPREL columns) and DEPS column *)
  let split ?(config=Conllx_config.sud) edge_list =
    match config.deps with
    | None -> (edge_list, [])
    | Some (deps_efn,_) ->
      let rec loop basic deps = function
        | [] -> (basic,deps)
        | edge::tail ->
          match String_map.find_opt deps_efn edge.label with
          | Some "yes" -> loop basic ({edge with label = (String_map.remove deps_efn edge.label)} :: deps) tail
          | Some x -> Error.error "Cannot interpret edge feature `%s=%s`, `yes` is the only available value for feature `%s`" deps_efn x deps_efn
          | None -> loop (edge::basic) deps tail
      in loop [] [] edge_list
end

(* ==================================================================================================== *)
module Parseme_mwes = struct

  type proj = Full | Proj_1 | Proj_2

  (* encoding of partial inclusion of multi-word in MWE *)
  (* example from [sequoia.deep_and_surf.parseme.frsemcor]
     1	Quant	quant	ADV	ADV	mwehead=P+D|mwelemma=quant_à	6	mod	_	_	1:P|MWE|CRAN	*
     2	au	à	P+D	P+D	component=y|s=def	1	dep_cpd	_	_	1/1	*
     3	sous-préfet	sous-préfet	N	NC	def=y|g=m|n=s|s=c	1	obj.p	_	_	2:NC|MWE|LEX	Person
  *)

  (* ---------------------------------------------------------------------------------------------------- *)
  let mwe_id_proj_of_string ?file ?sent_id ~line_num s =
    try
      match Str.bounded_split (Str.regexp "/") s 2 with
      | [id] -> (int_of_string id, Full)
      | [id; "1"] -> (int_of_string id, Proj_1)
      | [id; "2"] -> (int_of_string id, Proj_2)
      | _ -> Error.error ?file ?sent_id ~line_num "Cannot parse mwe_id: %s" s
    with Failure _ -> Error.error ?file ?sent_id ~line_num "Cannot parse mwe_id: %s" s

  (* ---------------------------------------------------------------------------------------------------- *)
  let mwe_id_proj_to_string (mwe_id,proj) =
    match proj with
    | Full -> sprintf "%d" mwe_id
    | Proj_1 -> sprintf "%d/1" mwe_id
    | Proj_2 -> sprintf "%d/2" mwe_id

  (* ---------------------------------------------------------------------------------------------------- *)
  type item = {
    parseme: string;
    mwepos: string option;
    label: string option;
    criterion: string option;
    ids: (Id.t * proj) list; (* ordered list of ids *)
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let empty = { mwepos = None; parseme = ""; label = None; criterion = None; ids = []; }

  (* ---------------------------------------------------------------------------------------------------- *)
  let id_map mapping t = { t with ids = List.map (fun (x,y) -> (List.assoc x mapping,y)) t.ids }

  (* ---------------------------------------------------------------------------------------------------- *)
  let item_to_json id item : Yojson.Basic.t = `Assoc (
      CCList.filter_map CCFun.id  [
        Some ("id", `String (sprintf "PARSEME_%d" id));
        (match item.parseme with ""  -> Error.error "Illegal MWE, no parseme field" | s -> Some ("parseme", `String s));
        (match item.mwepos with Some x -> Some ("mwepos", `String x) | None -> None);
        (match item.label with Some x -> Some ("label", `String x) | None -> None);
        (match item.criterion with Some x -> Some ("criterion", `String x) | None -> None)
      ]
    )

  (* ---------------------------------------------------------------------------------------------------- *)
  let item_of_json ids json =
    let open Yojson.Basic.Util in
    let parseme = json |> member "parseme" |> to_string in
    let mwepos = try Some (json |> member "mwepos" |> to_string) with _ -> None in
    let label = try Some (json |> member "label" |> to_string) with _ -> None  in
    let criterion = try Some (json |> member "criterion" |> to_string) with _ -> None in
    { parseme; mwepos; label; criterion; ids }

  (* ---------------------------------------------------------------------------------------------------- *)
  let item_of_string s =
    match Str.split (Str.regexp "|") s with

    (* usage of the PARSEME:MWE field in PARSEME project *)
    | [one] -> { empty with parseme="MWE"; mwepos= Some "VERB"; label=Some one }

    (* usage of the PARSEME:MWE field in PARSEME-FR project *)
    | [m;kl;c] ->
      let mwepos = match m with "_" -> None | s -> Some s in
      let (parseme, label) =
        match Str.split (Str.regexp "-") kl with
        | [k] -> (k, None)
        | [k; l] -> (k, Some l)
        | _ -> Error.error "Cannot parse PARSEME:MWE %s" s in
      let criterion = match c with "_" -> None | s -> Some s in
      { mwepos; parseme; label; criterion; ids=[] }
    | _ -> Error.error "Cannot parse PARSEME:MWE %s" s

  (* ---------------------------------------------------------------------------------------------------- *)
  let item_to_string item =
    sprintf "%s|%s|%s"
      (match item.mwepos with None -> "_" | Some s -> s)
      (match item.label with None -> item.parseme | Some s -> sprintf "%s-%s" item.parseme s)
      (match item.criterion with None -> "_" | Some s -> s)

  (* ---------------------------------------------------------------------------------------------------- *)
  type t = item Int_map.t

  (* ---------------------------------------------------------------------------------------------------- *)
  let update ?file ?sent_id ~line_num ~columns node_id item_list t =
    List.fold_left2
      (fun acc col item ->
         match (col, item) with
         | (Column.PARSEME_MWE, "*") -> acc
         | (Column.PARSEME_MWE, s) ->
           let mwe_items = Str.split (Str.regexp ";") s in
           List.fold_left
             (fun acc2 mwe_item ->
                match Str.bounded_split (Str.regexp ":") mwe_item 2 with
                | [id_proj] ->
                  let (mwe_id, proj) = mwe_id_proj_of_string ?file ?sent_id ~line_num id_proj in
                  begin
                    match Int_map.find_opt mwe_id acc2 with
                    | None -> Int_map.add mwe_id {empty with ids = [(node_id, proj)] } acc2
                    | Some item -> Int_map.add mwe_id { item with ids = (node_id, proj) :: item.ids } acc2
                  end
                | [id_proj; desc] ->
                  begin
                    let (mwe_id, proj) = mwe_id_proj_of_string ?file ?sent_id ~line_num id_proj in
                    let new_item = item_of_string desc in
                    match Int_map.find_opt mwe_id acc2 with
                    | None -> Int_map.add mwe_id {new_item with ids = [(node_id, proj)] } acc2
                    | Some item -> Int_map.add mwe_id { new_item with ids = (node_id, proj) :: item.ids } acc2
                  end
                | _ -> Error.error "cannot parse PARSEME_MWE"
             ) acc mwe_items
         | _ -> acc
      ) t columns item_list
end

(* ==================================================================================================== *)
module Conllx = struct
  type t = {
    meta: (string * string) list;
    nodes: Node.t list;
    order: Id.t list;
    edges: Edge.t list;
    parseme_mwes: Parseme_mwes.t;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let get_meta t = t.meta

  (* ---------------------------------------------------------------------------------------------------- *)
  let size t = List.length t.nodes

  (* ---------------------------------------------------------------------------------------------------- *)
  let find_node id t =
    match List.find_opt (fun n -> n.Node.id = id) t with
    | Some n -> n
    | None -> Error.error "BUG: inconsistent id: %s" (Id.to_string id)

  (* ---------------------------------------------------------------------------------------------------- *)

  (* when MISC features is used on a MWT line, there is no place to store the data in Conllx.t: we used a special meta as a hack *)
  let textform_up t =
    match Node.textform_up t.nodes with
    | (new_nodes, "") -> { t with nodes = new_nodes }
    | (new_nodes, mwt_misc) -> { t with nodes = new_nodes; meta=("##MWT_MISC##", mwt_misc)::t.meta }

  (* ---------------------------------------------------------------------------------------------------- *)
  let textform_down t =
    match List.assoc_opt "##MWT_MISC##" t.meta with
    | None -> { t with nodes = Node.textform_down "" t.nodes}
    | Some mwt_misc -> { t with nodes = Node.textform_down mwt_misc t.nodes; meta = List.remove_assoc "##MWT_MISC##" t.meta }

  (* ---------------------------------------------------------------------------------------------------- *)
  let wordform_up t = { t with nodes = Node.wordform_up t.nodes}

  (* ---------------------------------------------------------------------------------------------------- *)
  let wordform_down t = { t with nodes = Node.wordform_down t.nodes}

  (* ---------------------------------------------------------------------------------------------------- *)
  let get_sent_id_opt_meta meta =
    match List.assoc_opt "sent_id" meta with
    | Some id -> Some id
    | None -> match List.assoc_opt "source_sent_id" meta with
      | Some id ->
        begin
          match Str.split (Str.regexp " ") id with
          | [_; _; id] -> Some id
          | _ -> None
        end
      | None -> None

  (* ---------------------------------------------------------------------------------------------------- *)
  let get_sent_id_opt t = get_sent_id_opt_meta t.meta

  (* ---------------------------------------------------------------------------------------------------- *)
  let set_sent_id new_sent_id t =
    { t with meta = ("sent_id", new_sent_id) :: (List.remove_assoc "sent_id" t.meta) }

  (* ---------------------------------------------------------------------------------------------------- *)
  let parse_meta (_,t) =
    match Str.bounded_split (Str.regexp "# *\\| *= *") t 2 with
    | [key;value] -> (key,value)
    | _ -> ("", t)

  (* ---------------------------------------------------------------------------------------------------- *)
  let check_edge ?file ?sent_id nodes edge =
    match (List.exists (fun n -> n.Node.id = edge.Edge.src) nodes, List.exists (fun n -> n.Node.id = edge.Edge.tar) nodes) with
    | (true, true) -> ()
    | (false, _) -> Error.error ?file ?sent_id "Unknown identifier `%s`" (Id.to_string edge.Edge.src)
    | (_, false) -> Error.error ?file ?sent_id "Unknown identifier `%s`" (Id.to_string edge.Edge.tar)

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string_list ~config ?file ~columns string_list_rev =
    let (meta_lines, graph_lines_rev) =
      List.partition (fun (_,l) -> l <> "" && l.[0] = '#') string_list_rev in

    let meta = List.map parse_meta meta_lines in
    let sent_id = get_sent_id_opt_meta meta in

    let (nodes_without_root, edges, parseme_mwes) =
      List.fold_left
        (fun (acc_nodes, acc_edges, acc_mwes)  (line_num,graph_line) ->
           let item_list = Str.split (Str.regexp "\t") graph_line in
           let node = Node.of_item_list ?file ?sent_id ~line_num ~columns item_list in
           let edge_list = Edge.of_item_list ?file ?sent_id ~line_num ~config ~columns node.Node.id item_list in
           let new_acc_mwes = Parseme_mwes.update ?file ?sent_id ~line_num ~columns node.Node.id item_list acc_mwes in
           (
             node::acc_nodes,
             (edge_list @ acc_edges),
             new_acc_mwes
           )
        ) ([],[],Int_map.empty) graph_lines_rev in

    let nodes = Node.conll_root :: nodes_without_root in

    (* check Conll structure; duplicated edges, src and tar of edges *)
    begin
      let rec loop used_ids = function
        | [] -> ()
        | {Node.id=id}::tail when Id_set.mem id used_ids -> Error.error ?sent_id ?file "id `%s` is used twice" (Id.to_string id)
        | {Node.id=id}::tail -> loop (Id_set.add id used_ids) tail in
      try
        loop Id_set.empty nodes;
        List.iter (check_edge ?file ?sent_id nodes) edges;
      with Conllx_error e -> Error.reraise ?sent_id ?file e
    end;
    {
      meta = List.rev meta;
      nodes;
      order = []; (* [order] is computed after textform/wordform because of MWT "nodes" *)
      edges;
      parseme_mwes; (* TODO: need cleaning (ids order) *)
    }
    |> textform_up
    |> wordform_up
    |> (fun t -> { t with order = List.map (fun node -> node.Node.id) t.nodes;})
  (* NOTE: order is built from order on nodes in input data, not following numerical order. *)

  (* ---------------------------------------------------------------------------------------------------- *)
  let normalise_ids t =
    let is_empty id = Node.is_empty (find_node id t.nodes) in
    let mapping = Id.normalise_list is_empty t.order in

    let new_nodes = List.map (Node.id_map mapping) t.nodes in
    let new_edges = List.map (Edge.id_map mapping) t.edges in
    let new_parseme_mwes = Int_map.map (Parseme_mwes.id_map mapping) t.parseme_mwes in
    { t with nodes = new_nodes; edges=new_edges; parseme_mwes=new_parseme_mwes }

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string ?(config=Conllx_config.default) ?(columns=Conllx_columns.default) s =
    match List.rev (List.mapi (fun i l -> (i+1,l)) (Str.split (Str.regexp "\n") s)) with
    | (_,"") :: t -> of_string_list ~config ~columns t (* remove pending empty line, if any *)
    | l -> of_string_list ~columns ~config l

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_json (t: t) : Yojson.Basic.t =
    let token_nodes = List.map Node.to_json t.nodes in
    let token_edges = List.map Edge.to_json t.edges in

    let (nodes, edges) =
      Int_map.fold
        (fun mwe_id mwe_item (acc_nodes, acc_edges) ->
           ((Parseme_mwes.item_to_json mwe_id mwe_item) :: acc_nodes,
            List.fold_left (fun acc (token_id,proj) ->
                let pre_label = match proj with
                  | Parseme_mwes.Proj_1 -> ["proj", `String "1"]
                  | Parseme_mwes.Proj_2 -> ["proj", `String "2"]
                  | Parseme_mwes.Full -> [] in
                `Assoc [
                  ("src", `String (sprintf "PARSEME_%d" mwe_id));
                  ("label", `Assoc (("parseme", `String mwe_item.parseme) :: pre_label));
                  ("tar", `String (Id.to_string token_id));
                ] :: acc
              ) acc_edges mwe_item.ids
           )
        ) t.parseme_mwes (token_nodes, token_edges) in

    `Assoc
      (CCList.filter_map
         (function
           | _, [] -> None
           | key, l -> Some (key, `List l)
         )
         [
           "meta", List.map (fun (k,v) -> `Assoc ["key", `String k; "value", `String v]) t.meta;
           "nodes", nodes;
           "order", (List.map (fun id -> `String (Id.to_string id)) t.order);
           "edges", edges
         ]
      )

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_json json =
    let open Yojson.Basic.Util in
    let all_nodes = json |> member "nodes" |> to_list  in
    let (parseme_nodes, token_nodes) = List.partition (function `Assoc l when List.mem_assoc "parseme" l -> true | _ -> false) all_nodes in

    let all_edges = json |> member "edges" |> to_list |> List.map Edge.of_json in
    let (parseme_edges, token_edges) = List.partition (function e when String_map.mem "parseme" e.Edge.label -> true | _ -> false) all_edges in

    let parseme_mwes =
      CCList.foldi
        (fun acc i parseme_node ->
           let node_id = parseme_node |> member "id" |> to_string in
           let tokens = List.fold_left
               (fun acc2 token_edge ->
                  if Id.to_string token_edge.Edge.src = node_id
                  then
                    (
                      token_edge.Edge.tar,
                      match String_map.find_opt "proj" token_edge.Edge.label with
                      | Some "1" -> Parseme_mwes.Proj_1
                      | Some "2" -> Parseme_mwes.Proj_2
                      | _ -> Parseme_mwes.Full
                    ) :: acc2
                  else acc2
               ) [] parseme_edges in
           Int_map.add (i+1) (Parseme_mwes.item_of_json tokens parseme_node) acc
        ) Int_map.empty (List.rev parseme_nodes) in

    try
      {
        meta = json |> member "meta" |> to_list |> List.map (fun j -> (j |> member "key" |> to_string, j |> member "value" |> to_string));
        nodes = token_nodes |> List.map Node.of_json;
        order = json |> member "order" |> to_list |> List.map (function `String s -> Id.Raw s | _ -> Error.error ~data:json ~fct:"Conllx.of_json" "illformed json (order field)");
        edges = token_edges;
        parseme_mwes;
      }
    with Type_error _ -> Error.error ~fct:"Conllx.of_json" "illformed json"

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_buff ?sent_id ~config ~columns buff t =
    let down_t = t |> normalise_ids |> wordform_down |> textform_down in

    let t_without_root = { down_t with nodes = List.filter (fun node -> not (Node.is_conll_root node)) down_t.nodes} in

    let _ = List.iter
        (function
          | ("", meta) -> bprintf buff "%s\n" meta
          | (key,value) -> bprintf buff "# %s = %s\n" key value
        ) t_without_root.meta in

    let _ = List.iter
        (fun node ->
           let (basic_edges, desp_edges) = Edge.split ~config t_without_root.edges in

           let (head,deprel) =
             match List.filter (Edge.is_tar node.Node.id) basic_edges with
             | [] -> ("_", "_")
             | l -> (
                 String.concat "|" (List.map (fun e -> Id.to_string e.Edge.src) l),
                 String.concat "|" (List.map (fun e -> Conllx_label.to_string_robust ~config e.Edge.label) l)
               ) in
           let deps =
             match List.sort (Edge.compare ~config) (List.filter (Edge.is_tar node.Node.id) desp_edges) with
             | [] -> "_"
             | l -> String.concat "|" (List.map (fun e -> (Id.to_string e.Edge.src)^":"^(Conllx_label.to_string_robust ~config e.Edge.label)) l) in

           let parseme_mwe =
             match
               (Int_map.fold
                  (fun mwe_id parseme_item acc ->
                     match parseme_item.Parseme_mwes.ids with
                     | (head, proj)::_ when head = node.id ->
                       (sprintf "%s:%s" (Parseme_mwes.mwe_id_proj_to_string (mwe_id, proj)) (Parseme_mwes.item_to_string parseme_item)):: acc
                     | _::tail when List.mem_assoc node.id tail ->
                       let proj = List.assoc node.id tail in
                       (Parseme_mwes.mwe_id_proj_to_string (mwe_id, proj)) :: acc

                     | l -> acc
                  ) t_without_root.parseme_mwes []) with
             | [] -> "*"
             | l -> String.concat ";" l in

           bprintf buff "%s\n" (Node.to_conll ~config ~columns head deprel deps parseme_mwe node)

        ) t_without_root.nodes in
    ()

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string ?(config=Conllx_config.default) ?(columns=Conllx_columns.default) t =
    let buff = Buffer.create 32 in
    to_buff ~config ~columns buff t;
    Buffer.contents buff

end

(* ==================================================================================================== *)
module Conllx_corpus = struct

  type t = {
    columns: Conllx_columns.t;
    data: (string * Conllx.t) array;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let empty = { columns = []; data = [||] }

  (* ---------------------------------------------------------------------------------------------------- *)
  let get_data t = t.data

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string ?(config=Conllx_config.default) ?columns t =
    (* if the columns argument is given, it has priority on internal columms of the corpus *)
    let columns = match columns with Some c -> c | None -> t.columns in
    let buff = Buffer.create 32 in
    bprintf buff "%s\n" (Conllx_columns.to_string columns);
    Array.iter
      (fun (_,conll) ->
         let sent_id = Conllx.get_sent_id_opt conll in
         Conllx.to_buff ?sent_id ~config ~columns:t.columns buff conll;
      ) t.data;
    Buffer.contents buff

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_lines ~config ?columns ?file lines =
    match lines with
    | [] -> empty
    | ((line_num,head)::tail) as all ->
      let (columns, data_lines) =
        match (Conllx_columns.of_string ?file ~line_num head, columns) with
        | (None, None) -> (Conllx_columns.default, all)
        | (None, Some cols) -> (cols, all)
        | (Some cols, None) -> (cols,tail)
        | (Some c1, Some c2) when c1 = c2 -> (c1,tail)
        | (Some c1, Some c2) ->
          Error.error ?file ~line_num "Inconsistent columsn declaration\nin file   --> %s\nin config --> %s\n"
            (Conllx_columns.to_string c1) (Conllx_columns.to_string c2) in

      let cpt = ref 0 in
      let res = ref [] in

      let rev_locals = ref [] in
      let save_one () =
        begin
          try
            let conll = Conllx.of_string_list ?file ~config ~columns !rev_locals in
            incr cpt;
            let base = match file with Some f -> Filename.basename f | None -> "stdin" in
            let sent_id = match Conllx.get_sent_id_opt conll with Some id -> id | None -> sprintf "%s_%05d" base !cpt in
            res := (sent_id,conll) :: !res
          with Error.Skip -> ()
        end;
        rev_locals := [] in

      let _ =
        List.iter
          (fun (line_num,line) -> match line with
             | "" when !rev_locals = [] -> Error.warning ?file ~line_num "Illegal blank line";
             | "" -> save_one ()
             | _ -> rev_locals := (line_num,line) :: !rev_locals
          ) data_lines in

      if !rev_locals != []
      then (
        Error.warning ?file "No blank line at the end of the file";
        save_one ()
      );
      { columns; data=Array.of_list (List.rev !res) }

  (* ---------------------------------------------------------------------------------------------------- *)
  let load ?(config=Conllx_config.default) ?columns file = of_lines ~config ?columns ~file (Misc.read_lines file)

  (* ---------------------------------------------------------------------------------------------------- *)
  let load_list ?(config=Conllx_config.default) ?columns file_list =
    match List.map (load ~config ?columns) file_list with
    | [] -> empty
    | ({ columns }::tail) as l ->
      if List.for_all (fun {columns=p} -> p = columns) tail
      then { columns; data = Array.concat (List.map (fun c -> c.data) l) }
      else Error.error "All files must have the same columns declaration"

  (* ---------------------------------------------------------------------------------------------------- *)
  let read ?(config=Conllx_config.default) ?columns () = of_lines ~config ?columns (Misc.read_stdin ())

  (* ---------------------------------------------------------------------------------------------------- *)
  let sizes t =
    (
      Array.length t.data,
      Array.fold_left (fun acc (_,conll) -> acc + Conllx.size conll) 0 t.data
    )

end

(* ======================================================================================================================== *)
module Conllx_stat = struct
  module String_map = Map.Make (String)
  module String_set = Set.Make (String)

  type key = Upos | Xpos

  type t = ((int String_map.t) String_map.t) String_map.t    (* keys are label --> gov --> dep *)

  let get_labels map = String_map.fold (fun label _ acc -> String_set.add label acc) map String_set.empty

  let get_tags map =
    String_map.fold
      (fun _ map2 acc ->
         String_map.fold
           (fun gov map3 acc2 ->
              String_map.fold
                (fun dep _ acc3 ->
                   String_set.add dep acc3
                ) map3 (String_set.add gov acc2)
           ) map2 acc
      ) map String_set.empty

  let add3 dep stat3 =
    let old = try String_map.find dep stat3 with Not_found -> 0 in
    String_map.add dep (old+1) stat3

  let add2 gov dep stat2 =
    let old = try String_map.find gov stat2 with Not_found -> String_map.empty in
    String_map.add gov (add3 dep old) stat2

  let add label gov dep stat =
    let old = try String_map.find label stat with Not_found -> String_map.empty in
    String_map.add label (add2 gov dep old) stat

  let map_add_conll ~config key conll map =
    let edges = conll.Conllx.edges in
    List.fold_left
      (fun acc edge ->
         let gov_node = Conllx.find_node edge.Edge.src conll.nodes in
         let dep_node = Conllx.find_node edge.Edge.tar conll.nodes in
         let gov_pos = match List.assoc_opt (match key with Upos -> "upos" | Xpos -> "xpos") gov_node.Node.feats with Some x -> x | _ -> "_" in
         let dep_pos = match List.assoc_opt (match key with Upos -> "upos" | Xpos -> "xpos") dep_node.Node.feats with Some x -> x | _ -> "_" in
         match Conllx_label.to_string ~config edge.label with
         | Ok label -> add label gov_pos dep_pos acc
         | _ -> acc
      ) map edges

  let build ?(config=Conllx_config.default) key corpus =
    Array.fold_left
      (fun acc (_,conll) ->
         map_add_conll ~config key conll acc
      ) String_map.empty corpus.Conllx_corpus.data

  let dump map =
    String_map.iter
      (fun label map2 ->
         String_map.iter
           (fun gov map3 ->
              String_map.iter
                (fun dep value ->
                   Printf.printf "%s -[%s]-> %s ==> %d\n" gov label dep value
                ) map3
           ) map2
      ) map

  let get map gov label dep =
    try Some (map |> (String_map.find label) |> (String_map.find gov) |> (String_map.find dep))
    with Not_found -> None

  let get_total_gov map label gov =
    try
      let map_dep = map |> (String_map.find label) |> (String_map.find gov) in
      Some (String_map.fold (fun _ x acc -> x + acc) map_dep 0)
    with Not_found -> None

  let get_total_dep map label dep =
    try
      let map_map_gov = map |> (String_map.find label)  in
      match
        String_map.fold
          (fun _ map acc ->
             match String_map.find_opt dep map with
             | None -> acc
             | Some x -> x+acc
          ) map_map_gov 0 with
      | 0 -> None
      | i -> Some i
    with Not_found -> None

  let get_total map label =
    String_map.fold (fun _ map acc1 ->
        String_map.fold (fun _ x acc2 -> x + acc2) map acc1
      ) (String_map.find label map) 0

  let count_compare (tag1,count1) (tag2,count2) =
    match (count1, count2) with
    | (Some i, Some j) -> Stdlib.compare j i
    | (None, Some _) -> 1
    | (Some _, None) -> -1
    | (None, None) -> Stdlib.compare tag1 tag2

  let table buff corpus_id map label =
    let tags = get_tags map in
    let govs = String_set.fold
        (fun gov acc -> (gov, get_total_gov map label gov) :: acc
        ) tags [] in
    let sorted_govs = List.sort count_compare govs in

    let deps = String_set.fold
        (fun dep acc -> (dep, get_total_dep map label dep) :: acc
        ) tags [] in
    let sorted_deps = List.sort count_compare deps in

    bprintf buff "							<table>\n";
    bprintf buff "								<colgroup/>\n";
    String_set.iter (fun _ -> bprintf buff "								<colgroup/>\n") tags;
    bprintf buff "								<thead>\n";
    bprintf buff "									<tr>\n";
    bprintf buff "										<th>\n";
    bprintf buff "											<span>DEP⇨</span>\n";
    bprintf buff "											<br>\n";
    bprintf buff "											<span>⇩GOV</span>\n";
    bprintf buff "										</th>\n";
    bprintf buff "										<th><b>TOTAL</b></th>\n";
    List.iter (fun (dep,_) -> bprintf buff "										<th>%s</th>\n" dep) sorted_deps;
    bprintf buff "									</tr>\n";
    bprintf buff "								</thead>\n";
    bprintf buff "								<tbody>\n";


    bprintf buff "									<tr>\n";
    bprintf buff "										<th><b>TOTAL</b></th>\n";
    bprintf buff "										<td class=\"total\"><a href=\"../?corpus=%s&relation=%s\" class=\"btn btn-warning\" target=\"_blank\">%d</a></td>\n" corpus_id label(get_total map label);
    List.iter (fun (dep,count) ->
        bprintf buff "										<td class=\"total\">%s</td>\n"
          (match count with
           | Some i ->
             let url = sprintf "../?corpus=%s&relation=%s&target=%s" corpus_id label dep in
             sprintf "<a href=\"%s\" class=\"btn btn-success\" target=\"_blank\">%d</a>" url i
           | None -> "")
      ) sorted_deps;
    bprintf buff "									</tr>\n";

    List.iter (fun (gov, count) ->
        bprintf buff "									<tr>\n";
        bprintf buff "										<th>%s</th>\n" gov;

        bprintf buff "										<td class=\"total\">%s</td>\n"
          (match count with
           | Some i ->
             let url = sprintf "../?corpus=%s&relation=%s&source=%s" corpus_id label gov in
             sprintf "<a href=\"%s\" class=\"btn btn-success\" target=\"_blank\">%d</a>" url i
           | None -> "");

        List.iter (fun (dep,_) ->
            bprintf buff "										<td>%s</td>\n"
              (match get map gov label dep with
               | Some i ->
                 let url = sprintf "../?corpus=%s&relation=%s&source=%s&target=%s" corpus_id label gov dep in
                 sprintf "<a href=\"%s\" class=\"btn btn-primary\" target=\"_blank\">%d</a>" url i
               | None -> "")
          ) sorted_deps;
        bprintf buff "									</tr>\n";
      ) sorted_govs;
    bprintf buff "								</tbody>\n";
    bprintf buff "							</table>\n";
    ()

  let escape_dot s =
    s
    |> Str.global_replace (Str.regexp "\\.") "__"
    |> Str.global_replace (Str.regexp ":") "__"
    |> Str.global_replace (Str.regexp "@") "___"

  let to_html corpus_id map =
    let labels = get_labels map in
    let buff = Buffer.create 32 in
    bprintf buff "<!DOCTYPE html>\n";
    bprintf buff "<html lang=\"en\">\n";
    bprintf buff "<head>\n";
    bprintf buff "	<meta charset=\"utf-8\">\n";
    bprintf buff "	<title>%s</title>\n" corpus_id;
    bprintf buff "	<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n";
    bprintf buff "	<script src=\"https://code.jquery.com/jquery-3.4.1.min.js\"> </script>\n";
    bprintf buff "	<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\" integrity=\"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u\" crossorigin=\"anonymous\">\n";
    bprintf buff "	<script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js\" integrity=\"sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa\" crossorigin=\"anonymous\"></script>\n";
    bprintf buff "	<link rel=\"stylesheet\" type=\"text/css\" href=\"../css/tables.css\">\n";
    bprintf buff "</head>\n";
    bprintf buff "<body>\n";
    bprintf buff "	<div class=\"container\">\n";
    bprintf buff "		<div class=\"row\">\n";
    bprintf buff "			<h1>%s</h1>\n" corpus_id;
    bprintf buff "			<div role=\"tabpanel\">\n";
    bprintf buff "				<div class=\"col-sm-2\" style=\"height: 100vh; overflow-y: auto;\">\n";
    bprintf buff "					<ul class=\"nav nav-pills brand-pills nav-stacked\" role=\"tablist\">\n";
    String_set.iter
      (fun label ->
         let esc = escape_dot label in
         bprintf buff "						<li role=\"presentation\" class=\"brand-nav\"><a href=\"#%s\" aria-controls=\"#%s\" data-toggle=\"tab\">%s [%d]</a></li>\n"
           esc esc label (get_total map label)
      ) labels;

    bprintf buff "					</ul>\n";
    bprintf buff "				</div>\n";
    bprintf buff "				<div class=\"col-sm-10\">\n";
    bprintf buff "					<div class=\"tab-content\">\n";

    String_set.iter
      (fun label ->
         bprintf buff "						<div class=\"tab-pane\" id=\"%s\">\n" (escape_dot label);
         table buff corpus_id map label;
         bprintf buff "						</div>\n";
      ) labels;

    bprintf buff "					</div>\n";
    bprintf buff "				</div>\n";
    bprintf buff "			</div>\n";
    bprintf buff "		</div>\n";
    bprintf buff "	</div>\n";
    bprintf buff "</body>\n";
    bprintf buff "</html>\n";

    Buffer.contents buff

end
