open Printf

module String_map = CCMap.Make (String)
module String_set = Set.Make (String)
module Int_map = CCMap.Make (Int)

exception Conllx_error of Yojson.Basic.t

let strip s = 
  let len = String.length s in
  if len > 0 && Char.code s.[len-1] = 13
  then String.sub s 0 (len-1)
  else s


(* ==================================================================================================== *)
module Error = struct

  (* ---------------------------------------------------------------------------------------------------- *)
  let build_json ?file ?sent_id ?line_num ?fct ?data ?prev message =
    let opt_list = [
      Some ("message", `String message);
      (CCOption.map (fun x -> ("file", `String x)) file);
      (CCOption.map (fun x -> ("sent_id", `String x)) sent_id);
      (CCOption.map (fun x -> ("line", `Int x)) line_num);
      (CCOption.map (fun x -> ("function", `String x)) fct);
      (CCOption.map (fun x -> ("data", x)) data);
    ] in
    let prev_list = match prev with
      | None -> [("library", `String "Conllx")]
      | Some (`Assoc l) -> l
      | Some json -> ["ill_formed_error", json] in
    `Assoc ((CCList.filter_map (fun x-> x) opt_list) @ prev_list)

  (* ---------------------------------------------------------------------------------------------------- *)
  let warning_ ?quiet ?file ?sent_id ?line_num ?fct ?data ?prev message =
    match quiet with
    | Some true -> ()
    | _ -> Printf.eprintf "%s\n" (Yojson.Basic.pretty_to_string (build_json ?file ?sent_id ?line_num ?fct ?data ?prev message))

  (* ---------------------------------------------------------------------------------------------------- *)
  let warning ?quiet ?file ?sent_id ?line_num ?fct ?data ?prev = Printf.ksprintf (warning_ ?quiet ?file ?sent_id ?line_num ?fct ?data ?prev)

  (* ---------------------------------------------------------------------------------------------------- *)
  let error_ ?file ?sent_id ?line_num ?fct ?data ?prev message =
    let json = build_json ?file ?sent_id ?line_num ?fct ?data ?prev message in
    raise (Conllx_error json)

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
module Column = struct
  type t = ID | FORM | LEMMA | UPOS | XPOS | FEATS | HEAD | DEPREL | DEPS | MISC
         | PARSEME_MWE
         | FRSEMCOR_NOUN
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
    | FRSEMCOR_NOUN -> "FRSEMCOR:NOUN"
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
    | "FRSEMCOR:NOUN" -> FRSEMCOR_NOUN
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
  let frsemcor = [Column.ID; FORM; LEMMA; UPOS; XPOS; FEATS; HEAD; DEPREL; DEPS; MISC; PARSEME_MWE; FRSEMCOR_NOUN]
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
    deps: (string * char) option;     (* edge feature value name for DEPS column, prefix for compact notation *)    (* EUD *)
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let basic = {
    name = "basic";
    core = "1";
    extensions = [];
    prefixes = [];
    feats = [];
    deps = None;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let ud_features = [
    (* UD features collected from data 2.10  *)
    "Abbr"; "AdjType"; "AdpType"; "AdvType"; "Agglutination"; "Analyt"; "Animacy"; "Animacy[gram]"; "Antr"; "Aspect"; "Augm";
    "Case"; "Cfm"; "Clas"; "Class"; "Clitic"; "Clusivity"; "Clusivity[obj]"; "Clusivity[psor]"; "Clusivity[subj]"; "Compound"; "Comt"; 
    "ConjType"; "Connegative"; "Contrast"; "Contv"; "Corf"; "Decl"; "Definite"; "Definitizer"; "Degree"; "DegreeModQpm"; "Deixis";
    "DeixisRef"; "Deixis[psor]"; "Delib"; "Deo"; "Derivation"; "Determ"; "Detrans"; "Dev"; "Dialect"; "Dimin"; "Dist"; "Echo"; "Emph";
    "Emphatic"; "Evident"; "Excl"; "ExtPos"; "Foc"; "Focus"; "FocusType"; "Foreign"; "Form";
    "Gender"; "Gender[dat]"; "Gender[erg]"; "Gender[obj]"; "Gender[psor]"; "Gender[subj]"; "HebBinyan"; "HebExistential"; "Hum";
    "Hyph"; "Imprs"; "Incorp"; "InfForm"; "InflClass"; "InflClass[nominal]"; "Int"; "Intens"; "Intense"; "Intension";
    "LangId"; "Language"; "Link"; "Mood"; "Morph"; "Movement"; "Mutation"; "NCount"; "NameType"; "NegationType"; "Neutral";
    "Nomzr"; "NonFoc"; "NounBase"; "NounClass"; "NounType"; "NumForm"; "NumType"; "NumValue";
    "Number"; "Number[abs]"; "Number[dat]"; "Number[erg]"; "Number[obj]"; "Number[psed]"; "Number[psor]"; "Number[subj]";
    "Obl"; "Orth"; "PartForm"; "PartType"; "PartTypeQpm"; "Pcl";
    "Person"; "Person[abs]"; "Person[dat]"; "Person[erg]"; "Person[obj]"; "Person[psor]"; "Person[subj]";
    "Polarity"; "Polite"; "Polite[abs]"; "Polite[dat]"; "Polite[erg]"; "Position"; "Poss"; "Possessed";
    "Pred"; "Prefix"; "PrepCase"; "PrepForm"; "Priv"; "PronType"; "Proper"; "Pun"; "PunctSide"; "PunctType";
    "Recip"; "Red"; "Redup"; "Reflex"; "Reflex[obj]"; "Reflex[subj]"; "Rel"; "Report"; "Speech"; "Strength";
    "Style"; "SubGender"; "Subcat"; "Subordinative"; "Tense"; "Top"; "Trans"; "Tv"; "Typo"; "Uninflect";
    "Valency"; "Variant"; "Ventive"; "VerbClass"; "VerbForm"; "VerbStem"; "VerbType"; "Voice";

    (* SUD features *)
    "Shared";
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
  let sud = {
    name="sud";
    core = "1";
    extensions = [ ("2",':'); ("subsem", '$'); ("deep", '@') ];
    prefixes = [];
    feats = ud_features;
    deps = Some ("enhanced", 'E');
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let sequoia = {
    name="sequoia";
    core = "1";
    extensions = [ ("2",':') ];
    prefixes = [ ( "deep", 'D'); ("surf", 'S') ];
    feats = []; (* Sequoia does not use the MISC column *)
    deps = None;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let iwpt = {
    name="iwpt";
    core = "1";
    extensions = [ ("2",':'); ("3",':'); ("4",':'); ("deep", '@') ];
    prefixes = [];
    feats = ud_features;
    deps = Some ("enhanced", 'E');
  }

  let build = function
    | "sequoia" -> sequoia
    | "ud" -> ud
    | "sud" -> sud
    | "iwpt" -> iwpt
    | "basic" | "orfeo" -> basic
    | s -> Error.error "Unknown config `%s` (available values are: `basic`, `ud`, `sud`, `sequoia`, `orfeo`)" s

  let get_name t = t.name

  let remove_from_feats feature_name config =
    { config with feats = CCList.remove ~eq:(=) ~key:feature_name config.feats }

end


(* ==================================================================================================== *)
module Id = struct
  type t =
    | Simple of int
    | Mwt of int * int   (* (3,4) --> "3-4" *)
    | Empty of int * int (* (8,2) --> "8.2" *)
    | Unordered of string
    | Raw of string (* when built from json *)

  (* ---------------------------------------------------------------------------------------------------- *)
  let base = function
    | Simple i | Mwt (i,_) | Empty (i,_) -> Some i
    | Unordered _ -> None
    | Raw _ -> failwith "Raw id"

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string = function
    | Simple id -> sprintf "%d" id
    | Mwt (init,final) -> sprintf "%d-%d" init final
    | Empty (base,sub) -> sprintf "%d.%d" base sub
    | Unordered s -> sprintf "_%s" s
    | Raw s -> sprintf "%s" s

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare ?file ?sent_id t1 t2 =
    match (base t1, base t2) with
    | (Some i, Some j) when i=j ->
      begin
        match (t1, t2) with
        | (Mwt _, Simple _) -> -1 | (Simple _, Mwt _) -> 1
        | (Mwt _, Empty _) -> -1 | (Empty _, Mwt _) -> 1
        | (Simple _, Empty _) -> -1 | (Empty _, Simple _) -> 1
        | (Simple _, Simple _) -> 0
        | (Empty (_,sub1), Empty (_,sub2)) -> Stdlib.compare sub1 sub2
        | _ -> Error.error ?file ?sent_id "Invalid arg in Id.compare <%s> <%s>" (to_string t1) (to_string t2)
      end
    | (Some i, Some j) -> Stdlib.compare i j
    | (None, Some _) -> 1
    | (Some _, None) -> 1
    | _ -> Stdlib.compare t1 t2

  (* ---------------------------------------------------------------------------------------------------- *)

  (* mapping is the correpondance for ordered node to their new id, take into account Raw ==> Unordered *)
  let map_id mapping id =
    match List.assoc_opt id mapping with
    | Some new_id -> new_id
    | None ->
      match id with
      | Raw s -> Unordered s
      | Unordered s -> Unordered s
      | _ -> failwith "Check this !!!"


  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string ?file ?sent_id ?line_num s =
    if s <> "" && s.[0] = '_'
    then Unordered (CCString.drop 1 s)
    else
      try
        match Str.bounded_full_split (Str.regexp "[.-]") s 2 with
        | [Str.Text string_id] -> Simple (int_of_string string_id)
        | [Str.Text string_init; Str.Delim "-"; Str.Text string_final] ->
          Mwt (int_of_string string_init,int_of_string string_final)
        | [Str.Text string_base; Str.Delim "."; Str.Text string_sub] ->
          Empty (int_of_string string_base,int_of_string string_sub)
        | _ -> Error.error  ?file ?sent_id ?line_num "Cannot parse id %s" s
      with Failure _ -> Error.error ?file ?sent_id ?line_num "Cannot parse id %s" s
end

(* ==================================================================================================== *)
module Id_set = Set.Make (struct type t = Id.t let compare = Stdlib.compare end)

(* ==================================================================================================== *)
module Id_map = Map.Make (struct type t = Id.t let compare = Stdlib.compare end)


(* ==================================================================================================== *)

let remove_misc_prefix s =
  if String.length s > 8 && String.sub s 0 8 = "__MISC__" 
  then String.sub s 8 ((String.length s) - 8)
  else s

let lowercase_compare s1 s2 = Stdlib.compare (CCString.lowercase_ascii (remove_misc_prefix s1)) (CCString.lowercase_ascii (remove_misc_prefix s2))
module Fs_map = CCMap.Make (struct type t=string let compare = lowercase_compare end)
(* we need a special map to take into account the lowercase based comparison of CoNLL feats *)

module Fs = struct
  type t = string Fs_map.t
  (* ---------------------------------------------------------------------------------------------------- *)
  let empty = Fs_map.empty

  (* deal with UD features like "Number[psor]" encoded "Number__psor" in Grew to avoid clashes with Grew brackets usage *)
  let encode_feat_name s = Str.global_replace (Str.regexp "\\[\\([0-9a-z]+\\)\\]") "__\\1" s
  let decode_feat_name s = Str.global_replace (Str.regexp "__\\([0-9a-z]+\\)$") "[\\1]" s

  let add ?file ?sent_id ?line_num f v acc =
    let enc_f = encode_feat_name f in
    match Fs_map.find_opt enc_f acc with
    | None -> Fs_map.add enc_f v acc
    | Some v' when v=v' -> Error.error ?file ?sent_id ?line_num "The feature `%s` is declared twice with the same value `%s`" f v
    | Some v' -> Error.error ?file ?sent_id ?line_num "The feature `%s` is declared twice with the different values `%s` and `%s`" f v v'

  (* ---------------------------------------------------------------------------------------------------- *)
  let parse ?config ?file ?sent_id ?line_num misc s init =
    let misc_pref f = 
      match config with
      | Some c when misc && List.mem f c.Conllx_config.feats -> "__MISC__" ^ f
      | _ -> f in
    match s with
    | "_" -> init
    | _ ->
      List.fold_left
        (fun acc fv ->
           match Str.bounded_full_split (Str.regexp "=") fv 2 with
           | [Str.Text f; Str.Delim "="; Str.Text v] -> 
             add ?file ?sent_id ?line_num (misc_pref f) v acc
           | _ -> Error.error ?file ?sent_id ?line_num "Cannot parse feature `%s` (expected string: `feat=value`)" fv
        ) init (Str.split (Str.regexp "|") s)

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare (f1,_) (f2,_) = Stdlib.compare (CCString.lowercase_ascii f1) (CCString.lowercase_ascii f2)

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string feats =
    if Fs_map.is_empty feats
    then "_"
    else String.concat 
        "|" 
        (Fs_map.fold 
           (fun k v acc -> (k^"="^v) :: acc
           ) feats []
        )

  (* ---------------------------------------------------------------------------------------------------- *)
  let string_feats ~config misc feats =
    let feat_list =
      Fs_map.fold
        (fun key value acc ->
           match key with
           | "lemma" | "upos" | "xpos" -> acc
           | "__RAW_MISC__" when misc -> (key,value) :: acc
           | "__RAW_MISC__" -> acc
           | s when misc && String.length s > 8 && String.sub s 0 8 = "__MISC__" -> (String.sub s 8 ((String.length s) - 8),value) :: acc
           | s when s.[0] = '_' -> acc (* TODO: DOC (other columns like orfeo are encoded from "_xxx" feats)*)
           | _ when config.Conllx_config.feats = [] ->
             begin
               if misc
               then acc
               else (key,value) :: acc
             end
           | f when List.mem (decode_feat_name f) config.feats ->
             begin
               if misc
               then acc
               else (key,value) :: acc
             end
           | _ ->
             begin
               if misc
               then (key,value) :: acc
               else acc
             end
        ) feats [] in
    match feat_list with
    | [] -> "_"
    | l -> String.concat "|" 
             (CCList.rev_map (fun (f,v) -> if f = "__RAW_MISC__" then v else (decode_feat_name f)^"="^v) l)
end

(* ==================================================================================================== *)
module Node = struct

  type t = {
    id: Id.t;
    form: string;
    feats: Fs.t;
    wordform: string option;
    textform: string option;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let conll_root = { id = Id.Simple 0; form="__0__"; feats=Fs.empty; wordform=None; textform=None;}

  (* ---------------------------------------------------------------------------------------------------- *)
  let is_conll_root t = t.form = "__0__"

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare ?file ?sent_id n1 n2 = Id.compare ?file ?sent_id n1.id n2.id

  (* ---------------------------------------------------------------------------------------------------- *)
  let id_map mapping t = { t with id = Id.map_id mapping t.id}

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_item_list ?file ?sent_id ?line_num ~config ~columns item_list =
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
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> Fs.add ?file ?sent_id ?line_num "lemma" item acc_feats)
             | (Column.UPOS, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> Fs.add ?file ?sent_id ?line_num "upos" item acc_feats)
             | (Column.XPOS, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> Fs.add ?file ?sent_id ?line_num "xpos" item acc_feats)
             | (Column.FEATS,_) -> 
               let feats = Fs.parse ~config ?file ?sent_id ?line_num false item acc_feats in (acc_id_opt, acc_form, feats)
             | (Column.MISC,_) -> 
               let feats = 
                 try Fs.parse ~config ?file ?sent_id ?line_num true item acc_feats 
                 with Conllx_error _ -> Fs.add ?file ?sent_id ?line_num "__RAW_MISC__" item acc_feats
               in (acc_id_opt, acc_form, feats)
             | (Column.ORFEO_START, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> Fs.add ?file ?sent_id ?line_num "_start" item acc_feats)
             | (Column.ORFEO_STOP, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> Fs.add ?file ?sent_id ?line_num "_stop" item acc_feats)
             | (Column.ORFEO_SPEAKER, _) ->
               (acc_id_opt, acc_form, match item with "_" -> acc_feats | _ -> Fs.add ?file ?sent_id ?line_num "_speaker" item acc_feats)
             | _ -> (acc_id_opt, acc_form, acc_feats)
          ) (None, "" ,Fs.empty) columns item_list in
      match id_opt with
      | None -> Error.error ?file ?sent_id ?line_num "No id"
      | Some id -> { id; form; feats; wordform=None; textform=None; }
    with Invalid_argument _ ->
      Error.error ?file ?sent_id ?line_num
        "Wrong number of fields: %d instead of %d expected"
        (List.length item_list) (List.length columns)

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_json_item t =
    (Id.to_string t.id,
     `Assoc (
       CCList.filter_map CCFun.id
         (
           (Some ("form", `String t.form))
           :: (CCOption.map (fun v -> ("textform", `String v)) t.textform)
           :: (CCOption.map (fun v -> ("wordform", `String v)) t.wordform)
           :: (Fs_map.fold (fun (f:string) v acc -> Some (f, `String v) :: acc) t.feats [])
         ))
    )

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_json_item (id,json_fs) =
    let open Yojson.Basic.Util in
    try
      let feats =
        List.fold_right
          (fun (k,v) acc ->
             match k with
             | "form" | "wordform" | "textform" -> acc
             | _ -> Fs_map.add k (v |> to_string) acc
          ) (json_fs |> to_assoc) Fs.empty in

      let id = Id.Raw id in
      let form_opt = try Some (json_fs |> member "form" |> to_string) with Type_error _ -> None in
      let form = match (id, form_opt) with
        | (_, Some f) -> f
        | (Simple 0, None) -> "__0__"
        | (_, None) -> "__NOFORM__" in
      {
        id;
        form;
        feats;
        wordform = (try Some (json_fs |> member "wordform" |> to_string) with Type_error _ -> None);
        textform = (try Some (json_fs |> member "textform" |> to_string) with Type_error _ -> None);
      }
    with Type_error _ ->

    try {id = Id.Raw id; feats= Fs_map.add "label" (json_fs |> to_string) Fs_map.empty; form=""; wordform=None; textform=None}
    with Type_error _ ->
      Error.error ~fct:"Node.of_json_item" ~data:json_fs "illformed json"

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_conll ~config ~columns head deprel deps parseme_mwe frsemcor t =
    String.concat "\t"
      (List.map (function
           | Column.ID -> Id.to_string t.id
           | Column.FORM -> t.form
           | Column.LEMMA -> (match Fs_map.find_opt "lemma" t.feats with Some l -> l | None -> "_")
           | Column.UPOS -> (match Fs_map.find_opt "upos" t.feats with Some l -> l | None -> "_")
           | Column.XPOS -> (match Fs_map.find_opt "xpos" t.feats with Some l -> l | None -> "_")
           | Column.FEATS -> Fs.string_feats ~config false t.feats
           | Column.HEAD -> head
           | Column.DEPREL -> deprel
           | Column.DEPS -> deps
           | Column.MISC -> Fs.string_feats ~config true t.feats
           | Column.PARSEME_MWE -> parseme_mwe
           | Column.FRSEMCOR_NOUN -> frsemcor
           | Column.ORFEO_START -> (match Fs_map.find_opt "_start" t.feats with Some l -> l | None -> "_")
           | Column.ORFEO_STOP -> (match Fs_map.find_opt "_stop" t.feats with Some l -> l | None -> "_")
           | Column.ORFEO_SPEAKER -> (match Fs_map.find_opt "_speaker" t.feats with Some l -> l | None -> "_")
         ) columns)

  (* ---------------------------------------------------------------------------------------------------- *)
  type mwt_misc = ((int * int) * string Fs_map.t) list

  (* ---------------------------------------------------------------------------------------------------- *)
  let mwt_misc_to_string (mwt_misc: mwt_misc) =
    String.concat "||"
      (List.map
         (fun ((i,f),feats) ->
            sprintf "%d::%d::%s" i f (Fs.to_string feats)
         ) mwt_misc
      )

  (* ---------------------------------------------------------------------------------------------------- *)
  let mwt_misc_of_string s : mwt_misc =
    List.map
      (fun item ->
         match Str.split (Str.regexp_string "::") item with
         | [si; sj; string_feats] ->
           let feats = 
             try Fs.parse false string_feats Fs.empty 
             with Conllx_error _ -> Fs.add "__RAW_MISC__" string_feats Fs.empty
           in ((int_of_string si, int_of_string sj), feats)
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
      | ({ form="__0__" } as node) :: tail -> node :: (loop to_underscore tail)
      | ({ id=Id.Empty _; _ } as node) :: tail -> { node with textform = Some "_"} :: (loop to_underscore tail)
      | { id=Id.Mwt (init,final); form; feats; _} :: next :: tail ->
        begin
          if not (Fs_map.is_empty feats)
          then mwt_misc := ((init,final),feats) :: !mwt_misc
        end;
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
          | None -> 
            Error.error 
              ~fct:"find_original_textform" 
              "Cannot build CoNLL data from graph: inconsistent textform data in graph; the token <%d> (form=%s) does not have a `textform` whereas the next token as a \"_\" textform" i node.form
        end
      | None -> Error.error ~fct:"find_original_textform" "Cannot find node `%d`" i in

    let mwts =
      List.map
        (fun (i,j) ->
           match List.assoc_opt (i,j) mwt_misc with
           | None -> { id=Id.Mwt (i,j); form=unescape_form (find_original_textform i); feats=Fs.empty; wordform=None; textform=None }
           | Some feats -> { id=Id.Mwt (i,j); form=unescape_form (find_original_textform i); feats; wordform=None; textform=None }
        ) (loop (None, decr_in_span)) in

    List.sort compare (mwts @ new_node_list)

  (* ---------------------------------------------------------------------------------------------------- *)
  let wordform_up node_list =
    List.map
      (fun node ->
         match (node.id, Fs_map.find_opt "wordform" node.feats) with
         | _ when node.form = "__0__" -> node
         | (_, Some wf) -> { node with wordform = Some wf; feats = Fs_map.remove "wordform" node.feats }
         | (Empty _, _) -> { node with wordform = Some "__EMPTY__" }
         | (_, None) -> { node with wordform = Some (escape_form node.form) }
      ) node_list

  (* ---------------------------------------------------------------------------------------------------- *)
  let wordform_down node_list =
    List.map
      (fun node ->
         match node.wordform with
         | Some "__EMPTY__" ->
           { node with wordform = None; feats = Fs_map.add "wordform" "__EMPTY__" node.feats }
         | Some wf when (unescape_form wf) <> node.form ->
           { node with wordform = None; feats = Fs_map.add "wordform" wf node.feats }
         | Some _ -> { node with wordform = None }
         | None -> { node with wordform = None }
      ) node_list

  (* ---------------------------------------------------------------------------------------------------- *)
  let is_empty t = t.wordform = Some "__EMPTY__" && t.textform = Some "_"
end

(* ==================================================================================================== *)
module Conllx_label = struct

  (* feature structures for edge labels *)
  type t = string String_map.t

  let empty = String_map.empty

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
        | Some new_pos -> String_map.add feat (String.sub s p (new_pos-p)) (loop next (new_pos+1) tail) in

    (if CCString.contains s '='
     then
       begin
         List.fold_left
           (fun acc x -> 
              match Str.split (Str.regexp "=") x with
              | [k;v] -> String_map.add k v acc
              | _ -> Error.error "Cannot parse label `%s`" s
           ) String_map.empty (Str.split (Str.regexp ",") (CCString.drop pos s))
       end
     else loop config.core pos config.extensions)
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
    let feat_list =
      try [("1", js |> to_string)]
      with Type_error _ ->
        js |> to_assoc |> (List.map (fun (k,v) -> k, to_string v)) in
    String_map.of_list feat_list
end

(* ==================================================================================================== *)
module Edge = struct
  type t = {
    src: Id.t;
    label: Conllx_label.t;
    tar: Id.t;
    line_num: int option;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let id_map mapping t =
    let new_src = Id.map_id mapping t.src in
    let new_tar = Id.map_id mapping t.tar in
    {t with src=new_src; tar=new_tar }

  (* ---------------------------------------------------------------------------------------------------- *)
  let compare ~config e1 e2 =
    match (String_map.find_opt "kind" e1.label, String_map.find_opt "kind" e2.label) with
    | (Some "surf", None) | (None, Some "deep") | (Some "surf", Some "deep") -> -1
    | (None, Some "surf") | (Some "deep", None) | (Some "deep", Some "surf") -> 1
    | _ ->
      match Id.compare e1.src e2.src with
      | 0 -> Stdlib.compare (Conllx_label.to_string ~config e1.label) (Conllx_label.to_string ~config e2.label)
      | n -> n

  (* ---------------------------------------------------------------------------------------------------- *)
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
        | (Some "0", None) -> [
            { src=Id.of_string ?file ?sent_id ?line_num "0";
              label=Conllx_label.of_string ~config "root";
              tar;
              line_num;
            }
          ]

        | (Some id, None) when List.length (Str.split (Str.regexp "|") id) = 1 ->
          let src = Id.of_string ?file ?sent_id ?line_num id in
          [{ src; label=Conllx_label.empty; tar; line_num}]

        | (Some srcs, Some labels) ->
          let src_id_list = List.map (Id.of_string ?file ?sent_id ?line_num) (Str.split (Str.regexp "|") srcs) in
          let label_list = List.map (Conllx_label.of_string ~config) (Str.split (Str.regexp "|") labels) in
          begin
            try List.map2 (fun src label -> { src; label; tar; line_num}) src_id_list label_list
            with Invalid_argument _ -> 
            match (src_id_list, label_list) with 
            | ([src], []) -> [{ src; label=String_map.empty; tar; line_num}] (* handle cases with "empty" labels *)
            | _ -> Error.error ?file ?sent_id ?line_num "different number of items in HEAD/DEPREL spec"
          end
        | (None, None) -> []
        | _ -> Error.error ?file ?sent_id ?line_num "Invalid HEAD/DEPREL spec" in

      match sec_opt with
      | None -> head_dep_edges
      | Some deps ->
        List.fold_left
          (fun acc sec ->
             match Str.bounded_split (Str.regexp ":") sec 2 with
             | [src_string;label] -> { src=Id.of_string  ?file ?sent_id ?line_num src_string; label = Conllx_label.of_string_deps ?file ?sent_id ?line_num ~config label; tar; line_num} :: acc
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
        line_num = None;
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
module Parseme = struct

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
  let item_sort t = { t with ids = List.sort (fun (x,_) (y,_) -> Id.compare x y) t.ids }
  (* NB: the order used of order on Id but order given in the order Conllx field should be used
         but in this context, the orders are equivalent. TOCHECK! *)

  (* ---------------------------------------------------------------------------------------------------- *)
  let id_map mapping t = { t with ids = List.map (fun (x,y) -> (Id.map_id mapping x,y)) t.ids }

  (* ---------------------------------------------------------------------------------------------------- *)
  let item_to_json_item id item : (string * Yojson.Basic.t) =
    (sprintf "PARSEME_%d" id,
     `Assoc (
       CCList.filter_map CCFun.id  [
         (match item.parseme with ""  -> Error.error "Illegal MWE, no parseme field" | s -> Some ("parseme", `String s));
         (match item.mwepos with Some x -> Some ("mwepos", `String x) | None -> None);
         (match item.label with Some x -> Some ("label", `String x) | None -> None);
         (match item.criterion with Some x -> Some ("criterion", `String x) | None -> None)
       ]
     )
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

  let sort = Int_map.map item_sort

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
module Frsemcor = struct

  (* ---------------------------------------------------------------------------------------------------- *)
  type item = {
    frsemcor: string;
    head: Id.t option; (* there is always an head but option in used during construction when head is unknown *)
    tokens: Id.t list; (* ordered list of ids different from head *)
  }

  let empty = { frsemcor=""; head=None; tokens=[] }

  (* ---------------------------------------------------------------------------------------------------- *)
  let id_map mapping t =
    { t with
      head = (match t.head with None -> None | Some id -> Some (Id.map_id mapping id));
      tokens = List.map (Id.map_id mapping) t.tokens
    }

  (* ---------------------------------------------------------------------------------------------------- *)
  type t = item Int_map.t

  (* ---------------------------------------------------------------------------------------------------- *)

  (* use negative number for mono-token semcor annotation *)
  let last_fresh_sem_id = ref 0
  let get_fresh_sem_id () = decr last_fresh_sem_id; !last_fresh_sem_id

  let update ?file ?sent_id ~line_num ~columns node_id item_list t =
    List.fold_left2
      (fun acc col item ->
         match (col, item) with
         | (Column.FRSEMCOR_NOUN, "*") -> acc
         | (Column.FRSEMCOR_NOUN, s) ->
           let sem_items = Str.split (Str.regexp ";") s in
           List.fold_left
             (fun acc2 sem_item ->
                match Str.bounded_split (Str.regexp ":") sem_item 2 with
                | [one] ->
                  begin
                    match int_of_string_opt one with
                    | Some sem_id ->
                      begin
                        match Int_map.find_opt sem_id acc2 with
                        | None -> Int_map.add sem_id {empty with tokens = [node_id] } acc2
                        | Some item -> Int_map.add sem_id { item with tokens = node_id :: item.tokens } acc2
                      end
                    | None ->
                      Int_map.add (get_fresh_sem_id ()) { empty with frsemcor=one; head = Some node_id} acc2
                  end
                (* | [id_proj] ->
                   let (mwe_id, proj) = mwe_id_proj_of_string ?file ?sent_id ~line_num id_proj in
                   begin
                    match Int_map.find_opt mwe_id acc2 with
                    | None -> Int_map.add mwe_id {empty with ids = [(node_id, proj)] } acc2
                    | Some item -> Int_map.add mwe_id { item with ids = (node_id, proj) :: item.ids } acc2
                   end *)
                | [string_sem_id; frsemcor] ->
                  begin
                    let sem_id = int_of_string string_sem_id in
                    match Int_map.find_opt sem_id acc2 with
                    | None -> Int_map.add sem_id {empty with frsemcor; head = Some node_id } acc2
                    | Some item -> Int_map.add sem_id { item with frsemcor; head = Some node_id } acc2
                  end
                | _ -> Error.error "cannot parse FRSEMCOR_NOUN"
             ) acc sem_items
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
    parseme: Parseme.t;
    frsemcor: Frsemcor.t;
  }

  (* ---------------------------------------------------------------------------------------------------- *)
  let get_meta t = t.meta

  (* ---------------------------------------------------------------------------------------------------- *)
  let set_meta k v t = {t with meta = t.meta @ [(k,v)] }

  (* ---------------------------------------------------------------------------------------------------- *)
  let size t = (List.length t.nodes) - 1 (* Do not count the dummy __0__ node *)

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
    if Str.string_match (Str.regexp "# \\([^= ]*\\) = \\(.*\\)") t 0
    then (Str.matched_group 1 t, Str.matched_group 2 t)
    else ("", t)

  (* ---------------------------------------------------------------------------------------------------- *)
  let check_edge ?file ?sent_id nodes edge =
    match (List.exists (fun n -> n.Node.id = edge.Edge.src) nodes, List.exists (fun n -> n.Node.id = edge.Edge.tar) nodes) with
    | (true, true) -> ()
    | (false, _) -> Error.error ?file ?sent_id ?line_num:edge.Edge.line_num "Unknown src identifier `%s`" (Id.to_string edge.Edge.src)
    | (_, false) -> Error.error ?file ?sent_id ?line_num:edge.Edge.line_num "Unknown tar identifier `%s`" (Id.to_string edge.Edge.tar)

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string_list_rev ~config ?file ~columns string_list_rev =
    let (meta_lines, graph_lines_rev) =
      List.partition (fun (_,l) -> l <> "" && l.[0] = '#') string_list_rev in

    let meta = List.map parse_meta meta_lines in
    let sent_id = get_sent_id_opt_meta meta in

    let (nodes_without_root, edges, parseme, frsemcor) =
      List.fold_left
        (fun (acc_nodes, acc_edges, acc_parseme, acc_frsemcor) (line_num,graph_line) ->
           let item_list = Str.split (Str.regexp "\t") graph_line in
           let node = Node.of_item_list ?file ?sent_id ~line_num ~config ~columns item_list in
           let edge_list = Edge.of_item_list ?file ?sent_id ~line_num ~config ~columns node.Node.id item_list in
           let new_acc_parseme = Parseme.update ?file ?sent_id ~line_num ~columns node.Node.id item_list acc_parseme in
           let new_acc_frsemcor = Frsemcor.update ?file ?sent_id ~line_num ~columns node.Node.id item_list acc_frsemcor in
           (
             node::acc_nodes,
             (edge_list @ acc_edges),
             new_acc_parseme,
             new_acc_frsemcor
           )
        ) ([],[],Int_map.empty,Int_map.empty) graph_lines_rev in

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
      parseme;
      frsemcor;
    }
    |> textform_up
    |> wordform_up
    |> (fun t -> { t with order = List.map (fun node -> node.Node.id) t.nodes;})
  (* NOTE: order is built from order on nodes in input data, not following numerical order. *)

  (* ---------------------------------------------------------------------------------------------------- *)
  let normalise_ids t =
    let is_empty id = Node.is_empty (find_node id t.nodes) in

    let mapping =
      let rec loop = function
        | (_,[]) -> []
        | (Id.Simple pos, head::tail) when is_empty head -> let new_id = Id.Empty (pos, 1) in (head,new_id) :: (loop (new_id,tail))
        | (Empty (pos,i), head::tail) when is_empty head -> let new_id = Id.Empty (pos,i+1) in (head,new_id) :: (loop (new_id,tail))
        | (id, head::tail) ->
          begin
            match Id.base id with
            | Some pos -> let new_id = Id.Simple (pos + 1) in (head,new_id) :: (loop (new_id,tail))
            | None -> Error.error "BUG: normalise_list, please report"
          end in
      match t.order with
      | [] -> []
      | h::_ when Node.is_conll_root (find_node h t.nodes) -> loop (Simple (-1), t.order)
      | _ -> loop (Simple 0, t.order) in

    { t with
      nodes = List.map (Node.id_map mapping) t.nodes;
      edges = List.map (Edge.id_map mapping) t.edges;
      parseme = Int_map.map (Parseme.id_map mapping) t.parseme;
      frsemcor = Int_map.map (Frsemcor.id_map mapping) t.frsemcor;
      order = List.map (Id.map_id mapping) t.order;
    }

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_num_lines ?(config=Conllx_config.basic) ?(columns=Conllx_columns.default) lines =
    let (lines, columns) =
      match lines with
      | [] -> ([], columns)
      | (_,head)::tail ->
        begin
          match Conllx_columns.of_string head with
          | Some c -> (tail, c)
          | None -> (lines, columns)
        end in

    match List.rev lines with
    | (_,"") :: t -> of_string_list_rev ~config ~columns t (* remove pending empty line, if any *)
    | l -> of_string_list_rev ~columns ~config l


  (* ---------------------------------------------------------------------------------------------------- *)
  let of_lines ?(config=Conllx_config.basic) ?(columns=Conllx_columns.default) lines =
    of_num_lines ~config ~columns (List.mapi (fun i l -> (i+1,strip l)) lines)

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_string ?(config=Conllx_config.basic) ?(columns=Conllx_columns.default) s =
    of_lines ~config ~columns (Str.split (Str.regexp "\r\\|\n\\|\013\\|\010\013?") s)

  (* ---------------------------------------------------------------------------------------------------- *)
  let load ?(config=Conllx_config.basic) ?(columns=Conllx_columns.default) file =
    of_lines ~config ~columns (CCIO.(with_in file read_lines_l))

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_json (t: t) : Yojson.Basic.t =

    let node_items = List.map Node.to_json_item t.nodes in
    let edges = List.map Edge.to_json t.edges in

    (* replace old values of nodes, edges with new ones, taking into account parseme *)
    let (node_items, edges) =
      Int_map.fold
        (fun mwe_id mwe_item (acc_nodes, acc_edges) ->
           ((Parseme.item_to_json_item mwe_id mwe_item) :: acc_nodes,
            List.fold_left (fun acc (token_id,proj) ->
                let pre_label = match proj with
                  | Parseme.Proj_1 -> ["proj", `String "1"]
                  | Parseme.Proj_2 -> ["proj", `String "2"]
                  | Parseme.Full -> [] in
                `Assoc [
                  ("src", `String (sprintf "PARSEME_%d" mwe_id));
                  ("label", `Assoc (("parseme", `String mwe_item.parseme) :: pre_label));
                  ("tar", `String (Id.to_string token_id));
                ] :: acc
              ) acc_edges mwe_item.ids
           )
        ) t.parseme (node_items, edges) in

    (* replace old values of nodes, edges with new ones, taking into account frsemcor *)
    let (node_items, edges) =
      Int_map.fold
        (fun sem_id sem_item (acc_nodes, acc_edges) ->
           match sem_item.Frsemcor.head with
           | None ->
             let sent_id = List.assoc_opt "sent_id" t.meta in
             Error.error ?sent_id "No head for id `%d`" sem_id
           | Some head ->
             let node_id = sprintf "FRSEMCOR_%d" sem_id in
             let new_node_item = (node_id, `Assoc [("frsemcor", `String sem_item.frsemcor)]) in
             (
               new_node_item :: acc_nodes,
               `Assoc [("src", `String node_id); ("label", `Assoc [("frsemcor", `String "head")]); ("tar", `String (Id.to_string head));] ::
               (List.fold_left
                  (fun acc token_id ->
                     `Assoc [
                       ("src", `String node_id);
                       ("label", `Assoc [("frsemcor", `String "yes")]);
                       ("tar", `String (Id.to_string token_id));
                     ] :: acc
                  ) acc_edges sem_item.Frsemcor.tokens
               )
             )
        ) t.frsemcor (node_items, edges) in

    `Assoc
      (CCList.filter_map CCFun.id
         [
           (match t.meta with [] -> None | _ -> Some ("meta", `Assoc (List.map (fun (k,v) -> (k, `String v)) t.meta)));
           (match node_items with [] -> None | _ -> Some ("nodes", `Assoc node_items));
           (match edges with [] -> None | _ -> Some ("edges", `List edges));
           (match t.order with [] -> None | _ -> Some ("order", `List (List.map (fun id -> `String (Id.to_string id)) t.order)));
         ]
      )

  (* ---------------------------------------------------------------------------------------------------- *)
  let of_json json =
    let open Yojson.Basic.Util in
    let order =
      List.map (function
          | `String s -> Id.Raw s
          | _ -> Error.error ~data:json ~fct:"Conllx.of_json" "illformed json (order field)"
        ) (try json |> member "order" |> to_list with Type_error _ -> []) in

    let positions = CCList.foldi (fun acc i id -> Id_map.add id i acc) Id_map.empty order in

    let all_node_items = try json |> member "nodes" |> to_assoc with Type_error _ -> [] in
    let (token_node_items, parseme_node_items, frsemcor_node_items) =
      List.fold_right
        (fun (id,node) (token_acc, parseme_acc, frsemcor_acc) ->
           match node with
           | `Assoc l when List.mem_assoc "parseme" l -> (token_acc, (id,node)::parseme_acc, frsemcor_acc)
           | `Assoc l when List.mem_assoc "frsemcor" l -> (token_acc, parseme_acc, (id,node)::frsemcor_acc)
           | _ -> ((id,node) :: token_acc, parseme_acc, frsemcor_acc)
        ) all_node_items ([],[],[]) in

    let all_edges = List.map Edge.of_json (try json |> member "edges" |> to_list with Type_error _ -> [])  in

    let (token_edges, parseme_edges, frsemcor_edges) =
      List.fold_left
        (fun (token_acc, parseme_acc, frsemcor_acc) edge ->
           match edge with
           | _ when String_map.mem "parseme" edge.Edge.label -> (token_acc, edge::parseme_acc, frsemcor_acc)
           | _ when String_map.mem "frsemcor" edge.Edge.label -> (token_acc, parseme_acc, edge::frsemcor_acc)
           | _ -> (edge :: token_acc, parseme_acc, frsemcor_acc)
        ) ([],[],[]) all_edges in

    let parseme_items_unordered =
      List.map
        (fun (node_id, parseme_node) ->
           let tokens = List.fold_right
               (fun token_edge acc2 ->
                  if Id.to_string token_edge.Edge.src = node_id
                  then
                    (
                      token_edge.Edge.tar,
                      match String_map.find_opt "proj" token_edge.Edge.label with
                      | Some "1" -> Parseme.Proj_1
                      | Some "2" -> Parseme.Proj_2
                      | _ -> Parseme.Full
                    ) :: acc2
                  else acc2
               ) parseme_edges [] in
           Parseme.item_of_json tokens parseme_node
        ) parseme_node_items in

    let rec compare_list l1 l2 = match (l1, l2) with
      | [],[] -> 0
      | [], _ -> -1
      | _, [] -> 1
      | (h1,_)::t1, (h2,_)::t2 ->
        match Stdlib.compare (Id_map.find h1 positions) (Id_map.find h2 positions) with
        | 0 -> compare_list t1 t2
        | x -> x in

    let parseme_items = List.sort
        (fun p1 p2 -> compare_list p1.Parseme.ids p2.ids
        ) parseme_items_unordered in

    let parseme =
      CCList.foldi
        (fun acc i item -> Int_map.add (i+1) item acc)
        Int_map.empty
        parseme_items in

    let (frsemcor_singleton, frsemcor_multi_unordered) =
      List.fold_left
        (fun (acc_singleton, acc_multi) (node_id, frsemcor_node) ->
           let (head_list, not_head_list) =
             List.fold_left
               (fun (acc_head, acc_not_head) edge ->
                  if Id.to_string edge.Edge.src = node_id
                  then
                    begin
                      match String_map.find_opt "frsemcor" edge.Edge.label with
                      | Some "head" -> (edge.Edge.tar::acc_head, acc_not_head)
                      | _ -> (acc_head, edge.Edge.tar::acc_not_head)
                    end
                  else (acc_head, acc_not_head)
               ) ([],[]) frsemcor_edges in

           match (head_list,not_head_list) with
           | ([head], []) ->
             ({ Frsemcor.frsemcor = frsemcor_node |> member "frsemcor" |> to_string; head = Some head; tokens=[] } :: acc_singleton, acc_multi)
           | ([head], tokens) ->
             (acc_singleton, { Frsemcor.frsemcor = frsemcor_node |> member "frsemcor" |> to_string; head = Some head; tokens } :: acc_multi)
           | (l,_) -> Error.error "Not one head -> %d" (List.length head_list)
        ) ([],[]) frsemcor_node_items in

    let rec compare_list l1 l2 = match (l1, l2) with
      | [],[] -> 0
      | [], _ -> -1
      | _, [] -> 1
      | h1::t1, h2::t2 ->
        match Stdlib.compare (Id_map.find h1 positions) (Id_map.find h2 positions) with
        | 0 -> compare_list t1 t2
        | x -> x in

    let frsemcor_multi = List.sort
        (fun m1 m2 ->
           match (m1.Frsemcor.head, m2.head) with
           | (Some h1, Some h2) -> (* ordering: == 1 ==> by head *)
             begin
               match Stdlib.compare (Id_map.find h1 positions) (Id_map.find h2 positions) with
               | 0 -> (* ordering: same head == 2 ==> by label *)
                 begin
                   match Stdlib.compare m1.Frsemcor.frsemcor m2.Frsemcor.frsemcor with
                   | 0 -> (* ordering: same head, same label == 3 ==> compare tokens *)
                     compare_list m1.Frsemcor.tokens m2.Frsemcor.tokens
                   | x -> x
                 end
               | x -> x
             end
           | _ -> 0
        ) frsemcor_multi_unordered in

    let frsemcor =
      CCList.foldi
        (fun acc i item -> Int_map.add (i+1) item acc)
        (CCList.foldi
           (fun acc i item -> Int_map.add (-i-1) item acc)
           Int_map.empty
           frsemcor_singleton)
        frsemcor_multi in

    let meta =
      List.map
        (fun (k,v) -> (k, v |> to_string))
        (try json |> member "meta" |> to_assoc with Type_error _ -> []) in
    {
      meta;
      nodes = token_node_items |> List.map Node.of_json_item;
      order;
      edges = token_edges;
      parseme;
      frsemcor;
    }
    |> normalise_ids

  (* ---------------------------------------------------------------------------------------------------- *)
  let order_mwes t =

    let order_mwe mwe =
      let ordered_ids =
        List.fold_right
          (fun id acc ->
             match List.assoc_opt id mwe.Parseme.ids  with
             | Some v -> (id,v) :: acc
             | None -> acc
          ) t.order [] in
      { mwe with ids = ordered_ids } in

    { t with parseme = Int_map.map order_mwe t.parseme }

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_buff ?sent_id ~config ~columns buff t =

    let t = order_mwes t in

    let down_t = t |> wordform_down |> textform_down in

    let t_without_root = { down_t with nodes = List.filter (fun node -> not (Node.is_conll_root node)) down_t.nodes} in

    let _ = List.iter
        (function
          | ("", meta) -> bprintf buff "%s\n" meta
          | (key,value) -> bprintf buff "# %s = %s\n" key value
        ) t_without_root.meta in

    let _ = List.iter
        (fun node ->
           let (basic_edges, deps_edges) = Edge.split ~config t_without_root.edges in

           let (head,deprel) =
             match List.filter (Edge.is_tar node.Node.id) basic_edges with
             | [] -> ("_", "_")
             | l ->
               let l = List.sort (Edge.compare ~config) l in
               (
                 String.concat "|" (List.map (fun e -> Id.to_string e.Edge.src) l),
                 String.concat "|" (List.map (fun e -> Conllx_label.to_string_robust ~config e.Edge.label) l)
               ) in
           let deps =
             match List.sort (Edge.compare ~config) (List.filter (Edge.is_tar node.Node.id) deps_edges) with
             | [] -> "_"
             | l -> String.concat "|" (List.map (fun e -> (Id.to_string e.Edge.src)^":"^(Conllx_label.to_string_robust ~config e.Edge.label)) l) in


           let parseme_mwe =
             match
               (Int_map.fold
                  (fun mwe_id parseme_item acc ->
                     match parseme_item.Parseme.ids with
                     | (head, proj)::_ when head = node.id ->
                       (sprintf "%s:%s" (Parseme.mwe_id_proj_to_string (mwe_id, proj)) (Parseme.item_to_string parseme_item)):: acc
                     | _::tail when List.mem_assoc node.id tail ->
                       let proj = List.assoc node.id tail in
                       (Parseme.mwe_id_proj_to_string (mwe_id, proj)) :: acc
                     | l -> acc
                  ) t_without_root.parseme []) with
             | [] -> "*"
             | l -> String.concat ";" (List.rev l) in

           let frsemcor = match
               (Int_map.fold
                  (fun mwe_id frsemcor_item acc ->
                     match (frsemcor_item.Frsemcor.head, frsemcor_item.Frsemcor.tokens) with
                     | (Some h, []) when h = node.id -> frsemcor_item.Frsemcor.frsemcor :: acc
                     | (Some h, tokens) when h=node.id -> (sprintf "%d:%s" mwe_id frsemcor_item.Frsemcor.frsemcor) :: acc
                     | (_,tokens) when List.mem node.id tokens -> (sprintf "%d" mwe_id) :: acc
                     | l -> acc
                  ) t_without_root.frsemcor []) with
           | [] -> "*"
           | l -> String.concat ";" l in

           bprintf buff "%s\n" (Node.to_conll ~config ~columns head deprel deps parseme_mwe frsemcor node)

        ) t_without_root.nodes in
    ()

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string ?(config=Conllx_config.basic) ?(columns=Conllx_columns.default) t =
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
  let get_columns t = t.columns

  (* ---------------------------------------------------------------------------------------------------- *)
  let to_string ?(config=Conllx_config.basic) ?columns t =
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
  let of_lines ?(config=Conllx_config.basic) ?(quiet=false) ?log_file ?columns ?file lines =
    match lines with
    | [] -> empty
    | (head::tail) as all ->

      let (columns, data_lines, delta) =
        match (Conllx_columns.of_string ?file head, columns) with
        | (None, None) -> (Conllx_columns.default, all, 1)
        | (None, Some cols) -> (cols, all, 1)
        | (Some cols, None) -> (cols,tail, 2)
        | (Some c1, Some c2) when c1 = c2 -> (c1,tail, 2)
        | (Some c1, Some c2) ->
          Error.error ?file "Inconsistent columns declaration\nin file --> %s\nin config --> %s\n"
            (Conllx_columns.to_string c1) (Conllx_columns.to_string c2) in

      let cpt = ref 0 in
      let res = ref [] in

      let rev_locals = ref [] in
      let save_one () =
        begin
          try
            let conll = Conllx.of_string_list_rev ?file ~config ~columns !rev_locals in
            incr cpt;
            let base = match file with Some f -> Filename.basename f | None -> "stdin" in
            let sent_id = match Conllx.get_sent_id_opt conll with Some id -> id | None -> sprintf "%s_%05d" base !cpt in
            res := (sent_id,conll) :: !res
          with Conllx_error json ->
            begin
              match log_file with
              | None -> raise (Conllx_error json)
              | Some f when Sys.file_exists f ->
                let out_ch = open_out_gen [Open_append] 0o755 f in
                Printf.fprintf out_ch "%s" (Yojson.Basic.pretty_to_string json);
                close_out out_ch
              | Some f ->
                let out_ch = open_out f in
                Printf.fprintf out_ch "%s" (Yojson.Basic.pretty_to_string json);
                close_out out_ch
            end
        end;
        rev_locals := [] in

      let _ =
        List.iteri
          (fun i line -> 
             let line_num = i+delta in
             match strip line with
             | "" when !rev_locals = [] -> Error.warning ~line_num ~quiet ?file "Illegal blank line";
             | "" -> save_one ()
             | _ -> rev_locals := (line_num,line) :: !rev_locals
          ) data_lines in

      if !rev_locals != []
      then (
        Error.warning ~quiet ?file "No blank line at the end of the file";
        save_one ()
      );
      { columns; data=Array.of_list (List.rev !res) }

  (* ---------------------------------------------------------------------------------------------------- *)
  let load ?(config=Conllx_config.basic) ?quiet ?log_file ?columns file =
    let lines = CCIO.(with_in file read_lines_l) in
    of_lines ~config ?quiet ?log_file ?columns ~file lines

  (* ---------------------------------------------------------------------------------------------------- *)
  let load_list ?(config=Conllx_config.basic) ?quiet ?log_file ?columns file_list =
    match List.map (load ~config ?quiet ?columns ?log_file) file_list with
    | [] -> empty
    | ({ columns }::tail) as l ->
      if List.for_all (fun {columns=p} -> p = columns) tail
      then { columns; data = Array.concat (List.map (fun c -> c.data) l) }
      else Error.error "All files must have the same columns declaration"

  (* ---------------------------------------------------------------------------------------------------- *)
  let sizes t =
    (
      Array.length t.data,
      Array.fold_left (fun acc (_,conll) -> acc + Conllx.size conll) 0 t.data
    )

end

(* ======================================================================================================================== *)
module Conllx_stat = struct

  module Label_map = Map.Make (
    struct 
      type t = string
      let compare t1 t2 =
        let e1 = String.length t1 > 2 && String.sub t1 0 2 = "E:"
        and e2 = String.length t2 > 2 && String.sub t2 0 2 = "E:" in
        match (e1, e2) with
        | (true, false) -> 1
        | (false, true) -> -1
        | _ -> Stdlib.compare t1 t2
    end
    )

  type t = ((int String_map.t) String_map.t) Label_map.t    (* keys are label --> gov --> dep *)

  let get_tags map =
    Label_map.fold
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
    let old = try Label_map.find label stat with Not_found -> String_map.empty in
    Label_map.add label (add2 gov dep old) stat

  let map_add_conll ~config (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) conll map =
    let edges = conll.Conllx.edges in
    List.fold_left
      (fun acc edge ->
         let gov_node = Conllx.find_node edge.Edge.src conll.nodes in
         let dep_node = Conllx.find_node edge.Edge.tar conll.nodes in
         let gov_value =
           match Fs_map.find_opt gov_key gov_node.Node.feats with
           | Some x -> x
           | None ->
             match gov_subkey_opt with
             | None -> "_"
             | Some subkey ->
               match Fs_map.find_opt subkey gov_node.Node.feats with
               | Some x -> x
               | None -> "_" in
         let dep_value =
           match Fs_map.find_opt dep_key dep_node.Node.feats with
           | Some x -> x
           | None ->
             match dep_subkey_opt with
             | None -> "_"
             | Some subkey ->
               match Fs_map.find_opt subkey dep_node.Node.feats with
               | Some x -> x
               | None -> "_" in
         match Conllx_label.to_string ~config edge.label with
         | Ok label -> add label gov_value dep_value acc
         | _ -> acc
      ) map edges

  let build ?(config=Conllx_config.basic) (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) corpus =
    Array.fold_left
      (fun acc (_,conll) ->
         map_add_conll ~config (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) conll acc
      ) Label_map.empty corpus.Conllx_corpus.data

  let dump map =
    Label_map.iter
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
    try Some (map |> (Label_map.find label) |> (String_map.find gov) |> (String_map.find dep))
    with Not_found -> None

  let get_total_gov map label gov =
    try
      let map_dep = map |> (Label_map.find label) |> (String_map.find gov) in
      Some (String_map.fold (fun _ x acc -> x + acc) map_dep 0)
    with Not_found -> None

  let get_total_dep map label dep =
    try
      let map_map_gov = map |> (Label_map.find label)  in
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
      ) (Label_map.find label map) 0

  let count_compare (tag1,count1) (tag2,count2) =
    match (count1, count2) with
    | (Some i, Some j) -> Stdlib.compare j i
    | (None, Some _) -> 1
    | (Some _, None) -> -1
    | (None, None) -> Stdlib.compare tag1 tag2

  let  url_encode url =
    let buff = Buffer.create 32 in
    String.iter
      (function
        | ' ' -> bprintf buff "%s" "%20"
        | '/' -> bprintf buff "%s" "%2F"
        | '=' -> bprintf buff "%s" "%3D"
        | '[' -> bprintf buff "%s" "%5B"
        | ']' -> bprintf buff "%s" "%5D"
        | '{' -> bprintf buff "%s" "%7B"
        | '}' -> bprintf buff "%s" "%7D"
        | '|' -> bprintf buff "%s" "%7C"
        | '<' -> bprintf buff "%s" "%3C"
        | '>' -> bprintf buff "%s" "%3E"
        | ';' -> bprintf buff "%s" "%3B"
        | '\n' -> bprintf buff "%s" "%0A"
        | '\"' -> bprintf buff "%s" "%22"
        | c -> bprintf buff "%c" c
      ) url;
    Buffer.contents buff

  let url relation (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) gov_opt dep_opt =
    let gov_item =
      match gov_opt with
      | None -> ""
      | Some "_" -> sprintf "GOV [!%s]; " gov_key
      | Some value ->
        match gov_subkey_opt with
        | None -> sprintf "GOV [%s=\"%s\"]; " gov_key value
        | Some subkey -> sprintf "GOV [%s=\"%s\"/%s=\"%s\"]; " gov_key value subkey value in

    let dep_item =
      match dep_opt with
      | None -> ""
      | Some "_" -> sprintf "DEP [!%s]; " dep_key
      | Some value ->
        match dep_subkey_opt with
        | None -> sprintf "DEP [%s=\"%s\"]; " dep_key value
        | Some subkey -> sprintf "DEP [%s=\"%s\"/%s=\"%s\"]; " dep_key value subkey value in

    let pattern = sprintf "pattern { GOV -[%s]-> DEP; %s%s}" relation gov_item dep_item in
    url_encode pattern

  let table buff corpus_id (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) map label =
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
    bprintf buff "										<td class=\"total\"><a href=\"../?corpus=%s&pattern=%s\" class=\"btn btn-warning\" target=\"_blank\">%d</a></td>\n"
      corpus_id (url label (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) None None) (get_total map label);
    List.iter (fun (dep,count) ->
        bprintf buff "										<td class=\"total\">%s</td>\n"
          (match count with
           | Some i ->
             (* let url = sprintf "../?corpus=%s&relation=%s&target=%s" corpus_id label dep in *)
             let url = sprintf "../?corpus=%s&pattern=%s" corpus_id (url label (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) None (Some dep)) in
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
             (* let url = sprintf "../?corpus=%s&relation=%s&source=%s" corpus_id label gov in *)
             let url = sprintf "../?corpus=%s&pattern=%s" corpus_id (url label (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) (Some gov) None) in
             sprintf "<a href=\"%s\" class=\"btn btn-success\" target=\"_blank\">%d</a>" url i
           | None -> "");

        List.iter (fun (dep,_) ->
            bprintf buff "										<td>%s</td>\n"
              (match get map gov label dep with
               | Some i ->
                 (* let url = sprintf "../?corpus=%s&relation=%s&source=%s&target=%s" corpus_id label gov dep in *)
                 let url = sprintf "../?corpus=%s&pattern=%s" corpus_id (url label (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) (Some gov) (Some dep)) in
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
    |> Str.global_replace (Str.regexp "\\$") "__"
    |> Str.global_replace (Str.regexp "@") "___"

  let to_html corpus_id (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) map =
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
    Label_map.iter
      (fun label _ ->
         let esc = escape_dot label in
         bprintf buff "						<li role=\"presentation\" class=\"brand-nav\"><a href=\"#%s\" aria-controls=\"#%s\" data-toggle=\"tab\">%s [%d]</a></li>\n"
           esc esc label (get_total map label)
      ) map;

    bprintf buff "					</ul>\n";
    bprintf buff "				</div>\n";
    bprintf buff "				<div class=\"col-sm-10\">\n";
    bprintf buff "					<div class=\"tab-content\">\n";

    Label_map.iter
      (fun label _ ->
         bprintf buff "						<div class=\"tab-pane\" id=\"%s\">\n" (escape_dot label);
         table buff corpus_id (gov_key,gov_subkey_opt) (dep_key,dep_subkey_opt) map label;
         bprintf buff "						</div>\n";
      ) map;

    bprintf buff "					</div>\n";
    bprintf buff "				</div>\n";
    bprintf buff "			</div>\n";
    bprintf buff "		</div>\n";
    bprintf buff "	</div>\n";
    bprintf buff "</body>\n";
    bprintf buff "</html>\n";

    Buffer.contents buff

end
