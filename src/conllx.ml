open Printf

(* ==================================================================================================== *)
exception Conllx_error of Yojson.Basic.t
exception Skip
let robust = ref false

let build_json ?file ?sent_id ?line_num ?fct ?data ?prev message =
  let opt_list = [
    Some ("message", `String message);
    (CCOpt.map (fun x -> ("file", `String x)) file);
    (CCOpt.map (fun x -> ("sent_id", `String x)) sent_id);
    (CCOpt.map (fun x -> ("line", `Int x)) line_num);
    (CCOpt.map (fun x -> ("function", `String x)) fct);
    (CCOpt.map (fun x -> ("data", `String x)) data);
  ] in
  let prev_list = match prev with
    | None -> [("library", `String "Conllx")]
    | Some (`Assoc l) -> l
    | Some json -> ["ill_formed_error", json] in
  `Assoc ((CCList.filter_map (fun x-> x) opt_list) @ prev_list)

let warning_ ?file ?sent_id ?line_num ?fct ?data ?prev message =
  Printf.eprintf "%s\n" (Yojson.Basic.pretty_to_string (build_json ?file ?sent_id ?line_num ?fct ?data ?prev message))

let warning ?file ?sent_id ?line_num ?fct ?data ?prev = Printf.ksprintf (warning_ ?file ?sent_id ?line_num ?fct ?data ?prev)

let error_ ?file ?sent_id ?line_num ?fct ?data ?prev message =
  let json = build_json ?file ?sent_id ?line_num ?fct ?data ?prev message in
  if !robust
  then (Printf.eprintf "%s\n" (Yojson.Basic.pretty_to_string json); raise Skip)
  else raise (Conllx_error json)

let error ?file ?sent_id ?line_num ?fct ?data ?prev = Printf.ksprintf (error_ ?file ?sent_id ?line_num ?fct ?data ?prev)

let reraise ?file ?sent_id ?line_num ?fct ?data ?prev json =
  let new_json = match json with
    | `Assoc list -> `Assoc (
        list
        |> (fun l -> match file with None -> l | Some x -> ("file", `String x) :: (List.remove_assoc "file" l))
        |> (fun l -> match sent_id with None -> l | Some x -> ("sent_id", `String x) :: (List.remove_assoc "sent_id" l))
        |> (fun l -> match line_num with None -> l | Some x -> ("line_num", `String x) :: (List.remove_assoc "line_num" l))
        |> (fun l -> match fct with None -> l | Some x -> ("fct", `String x) :: (List.remove_assoc "fct" l))
        |> (fun l -> match data with None -> l | Some x -> ("data", `String x) :: (List.remove_assoc "data" l))
        |> (fun l -> match prev with None -> l | Some x -> ("prev", `String x) :: (List.remove_assoc "prev" l))
      )
    | _ -> error "Bug in json error structure" in
  raise (Conllx_error new_json)

(* ==================================================================================================== *)
module Misc = struct

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
           | _ -> error ?file ?sent_id ?line_num "Unknown feat %s" feat
        ) (Str.split (Str.regexp "|") s)

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
    with Sys_error msg -> error "%s" msg
end

(* ==================================================================================================== *)
module Column = struct
  type t = ID | FORM | LEMMA | UPOS | XPOS | FEATS | HEAD | DEPREL | DEPS | MISC | PARSEME_MWE

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
    | PARSEME_MWE-> "PARSEME:MWE"

  let from_string ?file ?line_num = function
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
    | x -> error ?file ?line_num "Unknown Column %s" x
end

(* ==================================================================================================== *)
module Profile = struct
  type t = Column.t list

  let default = [Column.ID; FORM; LEMMA; UPOS; XPOS; FEATS; HEAD; DEPREL; DEPS; MISC]

  let to_string t =
    sprintf "# global.columns = %s"
      (String.concat " " (List.map Column.to_string t))

  let from_string ?file ?line_num t =
    match Str.split (Str.regexp " ") t with
    | "#" :: "global.columns" :: "=" :: l -> Some (List.map (Column.from_string ?file ?line_num) l)
    | _ -> None
end

(* ==================================================================================================== *)
module Id = struct
  type t =
    | Simple of int
    | Mwt of int * int   (* (3,4) --> "3-4" *)
    | Empty of int * int (* (8,2) --> "8.2" *)

  let base = function Simple i | Mwt (i,_) | Empty (i,_) -> i

  let to_string = function
    | Simple id -> sprintf "%d" id
    | Mwt (init,final) -> sprintf "%d-%d" init final
    | Empty (base,sub) -> sprintf "%d.%d" base sub

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
        | _ -> error ?file ?sent_id "Invalid arg in Id.compare <%s> <%s>" (to_string t1) (to_string t2)
      end
    | n -> n

  (* rebuild ids list to follow the given order (taking into account empty nodes) *)
  let normalise_list is_empty id_list =
    let rec loop = function
      | (_,[]) -> []
      | (Simple pos, head::tail) when is_empty head -> let new_id = Empty (pos, 1) in (head,new_id) :: (loop (new_id,tail))
      | (Empty (pos,i), head::tail) when is_empty head -> let new_id = Empty (pos,i+1) in (head,new_id) :: (loop (new_id,tail))
      | (id, head::tail) -> let new_id = Simple ((base id)+1) in (head,new_id) :: (loop (new_id,tail))
    in
    loop (Simple (-1), id_list)

  let from_string s =
    try
      match Str.bounded_full_split (Str.regexp "[.-]") s 2 with
      | [Str.Text string_id] -> Simple (int_of_string string_id)
      | [Str.Text string_init; Str.Delim "-"; Str.Text string_final] ->
        Mwt (int_of_string string_init,int_of_string string_final)
      | [Str.Text string_base; Str.Delim "."; Str.Text string_sub] ->
        Empty (int_of_string string_base,int_of_string string_sub)
      | _ -> error "Cannot parse id %s" s
    with Failure _ -> error "Cannot parse id %s" s
end

module Id_set = Set.Make (struct type t = Id.t let compare = Stdlib.compare end)

(* ==================================================================================================== *)
module Feat = struct
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

  let compare (f1,_) (f2,_) = Stdlib.compare (CCString.lowercase_ascii f1) (CCString.lowercase_ascii f2)

  let string_feats misc feats =
    let filtered_feats = List.filter
        (function
          | ("lemma",_) | ("upos",_) | ("xpos",_) -> false
          | (f,_) when List.mem f ud_features -> not misc
          | _ -> misc
        ) feats in
    match filtered_feats with
    | [] -> "_"
    | _ -> String.concat "|" (List.map (fun (f,v) -> f^"="^v) filtered_feats)

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

  let conll_root = { id = Id.Simple 0; form="__ROOT__"; feats=[]; wordform=None; textform=None;}

  let is_conll_root t = t.form = "__ROOT__"

  let compare ?file ?sent_id n1 n2 = Id.compare ?file ?sent_id n1.id n2.id

  let id_map mapping t =
    match List.assoc_opt t.id mapping with
    | Some new_id -> {t with id = new_id}
    | None -> t

  let from_item_list ?file ?sent_id ?line_num profile item_list =
    try
      let (id_opt, form, feats) =
        List.fold_left2
          (fun (acc_id_opt, acc_form, acc_feats) col item ->
             match (col, acc_id_opt) with
             | (Column.ID, None) -> (Some (Id.from_string item), acc_form, acc_feats)
             | (Column.ID, _) -> error "Dup id"
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
             | _ -> (acc_id_opt, acc_form, acc_feats)
          ) (None, "" ,[]) profile item_list in
      match id_opt with
      | None -> error "No id"
      | Some id -> { id; form; feats = List.sort Feat.compare feats; wordform=None; textform=None; }
    with Invalid_argument _ ->
      error ?file ?sent_id ?line_num
        "Wrong number of fields: %d instead of %d expected"
        (List.length item_list) (List.length profile)

  let to_json t = `Assoc (
      CCList.filter_map CCFun.id
        (
          (Some ("id", `String (Id.to_string t.id)))
          :: (Some ("form", `String t.form))
          :: (CCOpt.map (fun v -> ("textform", `String v)) t.textform)
          :: (CCOpt.map (fun v -> ("wordform", `String v)) t.wordform)
          :: (List.map (fun (f,v) -> Some (f, `String v)) t.feats)
        ))

  let from_json (json: Yojson.Basic.t) =
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
      let id = json |> member "id" |> to_string |> Id.from_string in
      let form_opt = try Some (json |> member "form" |> to_string) with Type_error _ -> None in
      let form = match (id, form_opt) with
        | (_, Some f) -> f
        | (Simple 0, None) -> "__ROOT__"
        | (_, None) -> error ~fct:"Node.from_json" "missing form" in
      {
        id;
        form;
        feats;
        wordform = (try Some (json |> member "wordform" |> to_string) with Type_error _ -> None);
        textform = (try Some (json |> member "textform" |> to_string) with Type_error _ -> None);
      }
    with Type_error _ -> error ~fct:"Node.from_json" "illformed json"

  let to_conll profile head deprel deps t =
    String.concat "\t"
      (List.map (function
           | Column.ID -> Id.to_string t.id
           | Column.FORM -> t.form
           | Column.LEMMA -> (match List.assoc_opt "lemma" t.feats with Some l -> l | None -> "_")
           | Column.UPOS -> (match List.assoc_opt "upos" t.feats with Some l -> l | None -> "_")
           | Column.XPOS -> (match List.assoc_opt "xpos" t.feats with Some l -> l | None -> "_")
           | Column.FEATS -> Feat.string_feats false t.feats
           | Column.HEAD -> head
           | Column.DEPREL -> deprel
           | Column.DEPS -> deps
           | Column.MISC -> Feat.string_feats true t.feats
           | _ -> "_"
         ) profile)

  type mwt_misc = ((int * int) * (string * string) list) list

  let mwt_misc_to_string (mwt_misc: mwt_misc) =
    String.concat "||"
      (List.map
         (fun ((i,f),feats) ->
            sprintf "%d::%d::%s" i f (Feat.string_feats true feats)
         ) mwt_misc
      )

  let mwt_misc_from_string s : mwt_misc =
    List.map
      (fun item ->
         match Str.split (Str.regexp_string "::") item with
         | [si; sj; feats] -> ((int_of_string si, int_of_string sj), Misc.parse_features feats)
         | _ -> error "mwt_misc_from_string"
      )  (Str.split (Str.regexp_string "||") s)

  let escape_form = function "_" -> "UNDERSCORE" | x -> x
  let unescape_form = function "UNDERSCORE" -> "_" | x -> x

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

  let textform_down mwt_misc_string node_list =
    let mwt_misc = mwt_misc_from_string mwt_misc_string in
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
        (match node.textform with Some tf -> unescape_form tf | None -> error "wordform")
      | None -> error ~fct:"find_original_textform" "wordform node %d" i in

    let mwts =
      List.map
        (fun (i,j) ->
           match List.assoc_opt (i,j) mwt_misc with
           | None -> { id=Id.Mwt (i,j); form=unescape_form (find_original_textform i); feats=[]; wordform=None; textform=None }
           | Some feats -> { id=Id.Mwt (i,j); form=unescape_form (find_original_textform i); feats; wordform=None; textform=None }
        ) (loop (None, decr_in_span)) in

    List.sort compare (mwts @ new_node_list)

  let wordform_up node_list =
    List.map
      (fun node ->
         match (node.id, List.assoc_opt "wordform" node.feats) with
         | _ when node.form = "__ROOT__" -> node
         | (_, Some wf) -> { node with wordform = Some wf; feats = List.remove_assoc "wordform" node.feats }
         | (Empty _, _) -> { node with wordform = Some "_" }
         | (_, None) -> { node with wordform = Some (escape_form node.form) }
      ) node_list

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

  let is_empty t = t.wordform = Some "_" && t.textform = Some "_"
end

(* ==================================================================================================== *)
module Edge = struct
  type t = {
    src: Id.t;
    label: string;
    tar: Id.t;
  }

  let to_string e = sprintf "%s -[%s]-> %s" (Id.to_string e.src) e.label (Id.to_string e.tar)

  let id_map mapping t =
    let new_src = match List.assoc_opt t.src mapping with Some new_id -> new_id | None -> t.src in
    let new_tar = match List.assoc_opt t.tar mapping with Some new_id -> new_id | None -> t.tar in
    {t with src=new_src; tar=new_tar }

  let compare e1 e2 =
    match Id.compare e1.src e2.src with
    | 0 -> Stdlib.compare e1.label e2.label
    | n -> n
  let is_tar id edge = edge.tar = id

  let from_item_list ?file ?sent_id ?line_num profile tar item_list =
    try
      let (src_opt, label_opt) =
        List.fold_left2
          (fun (acc_src, acc_label) col item ->
             match (item, col, acc_src, acc_label) with
             | ("_",_,_,_) -> (acc_src, acc_label)
             | (_,Column.HEAD, None, _) -> (Some (Id.from_string item), acc_label)
             | (_,Column.DEPREL, _, None) -> (acc_src,Some item)
             | (_,Column.HEAD, _, _)
             | (_,Column.DEPREL, _, _) -> error ?file ?sent_id ?line_num "Invalid HEAD/DEPREL spec"
             | _ -> (acc_src, acc_label)
          ) (None, None) profile item_list in
      match (src_opt, label_opt) with
      | (Some src, Some label) -> Some { src; label; tar}
      | (None, None) -> None
      | _ -> error ?file ?sent_id ?line_num "Invalid HEAD/DEPREL spec"
    with Invalid_argument _ ->
      error ?file ?sent_id ?line_num
        "Wrong number of fields: %d instead of %d expected"
        (List.length item_list) (List.length profile)


  let sec_from_item_list ?file ?sent_id ?line_num profile tar item_list =
    try
      List.fold_left2
        (fun acc_sec_edges col item ->
           match col with
           | Column.DEPS ->
             begin
               match item with
               | "_" -> acc_sec_edges
               | _ -> acc_sec_edges @
                      (List.fold_left
                         (fun acc sec ->
                            match Str.bounded_split (Str.regexp ":") sec 2 with
                            | [src_string;label] -> { src=Id.from_string src_string; label; tar} :: acc
                            | _ -> error ?file ?sent_id ?line_num "Cannot parse secondary edges %s" sec
                         ) [] (Str.split (Str.regexp "|") item)
                      )
             end
           | _ -> acc_sec_edges
        ) [] profile item_list
    with Invalid_argument _ ->
      error ?file ?sent_id ?line_num
        "Wrong number of fields: %d instead of %d expected"
        (List.length item_list) (List.length profile)


  let to_json t = `Assoc [
      ("src", `String (Id.to_string t.src));
      ("label", `String t.label);
      ("tar", `String (Id.to_string t.tar));
    ]

  let from_json json =
    let open Yojson.Basic.Util in
    try
      {
        src = json |> member "src" |> to_string |> Id.from_string;
        label = json |> member "label" |> to_string;
        tar = json |> member "tar" |> to_string |> Id.from_string;
      }
    with Type_error _ -> error ~fct:"Edge.from_json" "illformed json"

end

(* ==================================================================================================== *)
module Conllx = struct
  type t = {
    meta: (string * string) list;
    nodes: Node.t list;
    order: Id.t list;
    edges: Edge.t list;
    sec_edges: Edge.t list;
  }

  (* ------------------------------------------------------------------------ *)
  let get_meta t = t.meta

  (* ------------------------------------------------------------------------ *)
  let find_node id t = List.find_opt (fun n -> n.Node.id = id) t

  (* ------------------------------------------------------------------------ *)
  (* when MISC features is used on a MWT line, there is no place to store the data in Conllx.t: we used a special meta as a hack *)
  let textform_up t =
    match Node.textform_up t.nodes with
    | (new_nodes, "") -> { t with nodes = new_nodes }
    | (new_nodes, mwt_misc) -> { t with nodes = new_nodes; meta=("##MWT_MISC##", mwt_misc)::t.meta }

  (* ------------------------------------------------------------------------ *)
  let textform_down t =
    match List.assoc_opt "##MWT_MISC##" t.meta with
    | None -> { t with nodes = Node.textform_down "" t.nodes}
    | Some mwt_misc -> { t with nodes = Node.textform_down mwt_misc t.nodes; meta = List.remove_assoc "##MWT_MISC##" t.meta }

  (* ------------------------------------------------------------------------ *)
  let wordform_up t = { t with nodes = Node.wordform_up t.nodes}

  (* ------------------------------------------------------------------------ *)
  let wordform_down t = { t with nodes = Node.wordform_down t.nodes}

  (* ------------------------------------------------------------------------ *)
  let get_sent_id_opt t = List.assoc_opt "sent_id" t.meta

  (* ------------------------------------------------------------------------ *)
  let parse_meta (_,t) =
    match Str.bounded_split (Str.regexp "# *\\| *= *") t 2 with
    | [key;value] -> (key,value)
    | _ -> ("", t)

  (* ------------------------------------------------------------------------ *)
  let check_edge nodes edge =
    match (List.exists (fun n -> n.Node.id = edge.Edge.src) nodes, List.exists (fun n -> n.Node.id = edge.Edge.tar) nodes) with
    | (true, true) -> ()
    | (false, _) -> error "Unknown identifier `%s`" (Id.to_string edge.Edge.src)
    | (_, false) -> error "Unknown identifier `%s`" (Id.to_string edge.Edge.tar)

  (* ------------------------------------------------------------------------ *)
  let from_string_list ?file ?(profile=Profile.default) string_list_rev =
    let (meta_lines, graph_lines_rev) =
      List.partition (fun (_,l) -> l <> "" && l.[0] = '#') string_list_rev in

    let meta = List.map parse_meta meta_lines in
    let sent_id = List.assoc_opt "sent_id" meta in

    let (nodes_without_root, edges, sec_edges) =
      List.fold_left
        (fun (acc_nodes, acc_edges, acc_sec_edges)  (line_num,graph_line) ->
           let item_list = Str.split (Str.regexp "\t") graph_line in
           let node = Node.from_item_list ?file ?sent_id ~line_num profile item_list in
           let edge_opt = Edge.from_item_list ?file ?sent_id ~line_num profile node.Node.id item_list in
           let sec_edges = Edge.sec_from_item_list ?file ?sent_id ~line_num profile node.Node.id item_list in
           (
             node::acc_nodes,
             (match edge_opt with Some new_edge -> new_edge::acc_edges | _ -> acc_edges),
             (sec_edges @ acc_sec_edges)
           )
        ) ([],[],[]) graph_lines_rev in

    let nodes = Node.conll_root :: nodes_without_root in

    (* check Conll structure; duplicated edges, src and tar of edges *)
    begin
      let rec loop used_ids = function
        | [] -> ()
        | {Node.id=id}::tail when Id_set.mem id used_ids -> error ?sent_id ?file "id `%s` is used twice" (Id.to_string id)
        | {Node.id=id}::tail -> loop (Id_set.add id used_ids) tail in
      try
        loop Id_set.empty nodes;
        List.iter (check_edge nodes) edges;
        List.iter (check_edge nodes) sec_edges
      with Conllx_error e -> reraise ?sent_id ?file e
    end;
    {
      meta = List.rev meta;
      nodes;
      order = []; (* [order] is computed after textform/wordform because of MWT "nodes" *)
      edges;
      sec_edges;
    }
    |> textform_up
    |> wordform_up
    |> (fun t -> { t with order = List.map (fun node -> node.Node.id) t.nodes;})
  (* NOTE: order is built from order on nodes in input data, not following numerical order. *)

  let normalise_ids t =
    let is_empty id = match find_node id t.nodes with
      | Some node -> Node.is_empty node
      | None -> error "inconsistent id: %s" (Id.to_string id) in
    let mapping = Id.normalise_list is_empty t.order in

    let new_nodes = List.map (Node.id_map mapping) t.nodes in
    let new_edges = List.map (Edge.id_map mapping) t.edges in
    let new_sec_edges = List.map (Edge.id_map mapping) t.sec_edges in
    { t with nodes = new_nodes; edges=new_edges; sec_edges= new_sec_edges}

  (* ------------------------------------------------------------------------ *)
  let from_string ?profile s =
    from_string_list ?profile
      (List.rev (List.mapi (fun i l -> (i+1,l)) (Str.split (Str.regexp "\n") s)))

  (* ------------------------------------------------------------------------ *)
  let to_json (t: t) : Yojson.Basic.t =
    `Assoc
      (CCList.filter_map
         (function
           | _, [] -> None
           | key, l -> Some (key, `List l)
         )
         [
           "meta", List.map (fun (k,v) -> `Assoc ["key", `String k; "value", `String v]) t.meta;
           "nodes", List.map Node.to_json t.nodes;
           "order", (List.map (fun id -> `String (Id.to_string id)) t.order);
           "edges", List.map Edge.to_json t.edges;
           "sec_edges", List.map Edge.to_json t.sec_edges;
         ]
      )

  (* ------------------------------------------------------------------------ *)
  let from_json json =
    let open Yojson.Basic.Util in
    try
      {
        meta = json |> member "meta" |> to_list |> List.map (fun j -> (j |> member "key" |> to_string, j |> member "value" |> to_string));
        nodes = json |> member "nodes" |> to_list |> List.map Node.from_json;
        order = json |> member "order" |> to_list |> List.map (function `String s -> Id.from_string s | _ -> error ~fct:"Conllx.from_json" "illformed json (order field)");
        edges = json |> member "edges" |> to_list |> List.map Edge.from_json;
        sec_edges = try json |> member "sec_edges" |> to_list |> List.map Edge.from_json with Type_error _ -> [];
      }
    with Type_error _ -> error ~fct:"Conllx.from_json" "illformed json"

  (* ------------------------------------------------------------------------ *)
  let to_buff ?sent_id buff ?(profile = Profile.default) t =
    let down_t = t |> normalise_ids |> wordform_down |> textform_down in

    let t_without_root = { down_t with nodes = List.filter (fun node -> not (Node.is_conll_root node)) down_t.nodes} in

    let _ = List.iter
        (function
          | ("", meta) -> bprintf buff "%s\n" meta
          | (key,value) -> bprintf buff "# %s = %s\n" key value
        ) t_without_root.meta in
    let _ = List.iter
        (fun node ->
           let (head,deprel) =
             match List.filter (Edge.is_tar node.Node.id) t_without_root.edges with
             | [] -> ("_", "_")
             | [one] -> (Id.to_string one.Edge.src, one.Edge.label)
             | _ -> error ?sent_id "more the one edge with one tar" in
           let deps =
             match List.sort Edge.compare (List.filter (Edge.is_tar node.Node.id) t_without_root.sec_edges) with
             | [] -> "_"
             | l -> String.concat "|" (List.map (fun e -> (Id.to_string e.Edge.src)^":"^e.Edge.label) l) in
           bprintf buff "%s\n" (Node.to_conll profile head deprel deps node)
        ) t_without_root.nodes in
    ()

  (* ------------------------------------------------------------------------ *)
  let to_string ?profile t =
    let buff = Buffer.create 32 in
    to_buff buff ?profile t;
    Buffer.contents buff

end

(* ==================================================================================================== *)
module Corpusx = struct
  type t = {
    profile: Profile.t;
    data: (string * Conllx.t) array;
  }
  let empty = { profile = []; data = [||] }

  let get_data t = t.data

  let to_string t =
    let buff = Buffer.create 32 in
    bprintf buff "%s\n" (Profile.to_string t.profile);
    Array.iter
      (fun (_,conll) ->
         let sent_id = Conllx.get_sent_id_opt conll in
         Conllx.to_buff ?sent_id buff ~profile:t.profile conll;
      ) t.data;
    Buffer.contents buff


  let load file =
    match Misc.read_lines file with
    | [] -> empty
    | ((line_num,h)::t) as all ->
      let (profile, data_lines) = match Profile.from_string ~file ~line_num h with
        | Some p -> (p,t)
        | None -> (Profile.default, all) in

      let cpt = ref 0 in
      let res = ref [] in

      let rev_locals = ref [] in
      let save_one () =
        begin
          try
            let conll = Conllx.from_string_list ~file ~profile !rev_locals in
            incr cpt;
            let base = Filename.basename file in
            let sent_id = match Conllx.get_sent_id_opt conll with Some id -> id | None -> sprintf "%s_%05d" base !cpt in
            res := (sent_id,conll) :: !res
          with Skip -> ()
        end;
        rev_locals := [] in

      let _ =
        List.iter
          (fun (line_num,line) -> match line with
             | "" when !rev_locals = [] -> warning ~file ~line_num "Illegal blank line";
             | "" -> save_one ()
             | _ -> rev_locals := (line_num,line) :: !rev_locals
          ) data_lines in

      if !rev_locals != []
      then (
        warning ~file "No blank line at the end of the file";
        save_one ()
      );
      { profile; data=Array.of_list (List.rev !res) }
end

