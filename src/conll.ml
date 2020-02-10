open Printf
open Log

open Utils
open Conll_types

(* ======================================================================================================================== *)
exception Conll_error of Yojson.Basic.t

let error ?file ?sent_id ?line ?fct ?data ?prev ?msg () =
  let opt_list = [
    (CCOpt.map (fun x -> ("message", `String x)) msg);
    (CCOpt.map (fun x -> ("file", `String x)) file);
    (CCOpt.map (fun x -> ("sent_id", `String x)) sent_id);
    (CCOpt.map (fun x -> ("line", `Int x)) line);
    (CCOpt.map (fun x -> ("function", `String x)) fct);
    (CCOpt.map (fun x -> ("data", `String x)) data);
  ] in
  let prev_list = match prev with
    | None -> [("library", `String "Conll")]
    | Some (`Assoc l) -> l
    | Some json -> ["ill_formed_error", json] in
  let json = `Assoc ((CCList.filter_map (fun x-> x) opt_list) @ prev_list) in
  raise (Conll_error json)

(* ======================================================================================================================== *)
module Sentence = struct
  let fr_clean_spaces with_spaces =
    List.fold_left
      (fun acc (regexp,repl) ->
         Str.global_replace regexp repl acc
      )
      with_spaces
      [
        Str.regexp_string " - ", " **DASH** ";
        Str.regexp "^- ", " **DASH** ";
        Str.regexp_string " -t-", "-t-";
        Str.regexp_string "_-_", "-";
        Str.regexp_string "_", " ";
        Str.regexp_string "' ", "'";
        Str.regexp_string " ,", ",";
        Str.regexp_string " .", ".";
        Str.regexp_string " %", "%";
        Str.regexp_string "( ", "(";
        Str.regexp_string " )", ")";
        Str.regexp_string "[ ", "[";
        Str.regexp_string " ]", "]";
        Str.regexp_string "- ", "-";
        Str.regexp_string " -", "-";
        Str.regexp_string " %", "%";
        Str.regexp_string " / ", "/";
        Str.regexp_string "\\\"", "\"";

        (* pairs of quotes *)
        Str.regexp "\" \\([^\"]*\\) \"", "\"\\1\"";
        Str.regexp " \" \\([a-zA-Z]\\)", " \"\\1";
        Str.regexp "\\([a-zA-Z]\\) \"\\([ ,.]\\)", "\\1\"\\2";
        Str.regexp " \"$", "\"";
        Str.regexp "^\" ", "\"";

        Str.regexp "\\([0-9]+\\) h \\([0-9]+\\)", "\\1h\\2";
        Str.regexp "\\([0-9]+\\) h\\([^a-z]\\)", "\\1h\\2";
        Str.regexp_string "**DASH**", "-";

        Str.regexp " - \\(.*\\) - ", " -\\1- ";

        Str.regexp "^ ", "";
      ]
end


(* ======================================================================================================================== *)
module Id_with_proj = struct
  (* t = (3, Some 1) <--> 3/1 conllid=3, partial_proj=1 *)
  type t = Id.t * int option

  let compare = Stdlib.compare

  let shift delta (i,p) = (Id.shift delta i, p)
end

(* ======================================================================================================================== *)
module Id_with_proj_set = Set.Make (Id_with_proj)

(* ======================================================================================================================== *)
module Mwe = struct
  type kind = Ne | Mwe

  let string_of_kind = function
    | Ne -> "NE"
    | Mwe -> "MWE"

  type t = {
    mwepos: string option;
    kind: kind;
    label: string option;
    criterion: string option;
    first: Id_with_proj.t;
    items: Id_with_proj_set.t;
  }

  let to_string t =
    let kind = string_of_kind t.kind in
    let long_label = match t.label with
      | None -> kind
      | Some l -> sprintf "%s-%s" kind l in
    sprintf "%s|%s|%s"
      (match t.mwepos with None -> "_" | Some l -> l)
      long_label
      (match t.criterion with None -> "_" | Some c -> c)

  let parse conll_id proj_opt s =
    match Str.split (Str.regexp "|") s with
    | [p; kind_label; crit] ->
      let mwepos = match p with "_" -> None | s -> Some s in
      let criterion = match crit with "_" -> None | s -> Some s in
      let (kind,label) =
        match Str.split (Str.regexp "-") kind_label with
        | ["MWE"] -> (Mwe, None)
        | ["MWE"; l]-> (Mwe, Some l)
        | ["NE"; l] -> (Ne, Some l)
        | _ -> error ~msg:(sprintf "mwe: cannot interpret MWE/NE description \"%s\"" s) () in
      {mwepos; kind; label; criterion; first=(conll_id,proj_opt); items=Id_with_proj_set.empty}
    | [l] ->
      {mwepos=None; kind=Mwe; label=Some l; criterion=None; first=(conll_id,proj_opt); items=Id_with_proj_set.empty}
    | _ -> error ~msg:(sprintf "mwe: cannot interpret MWE/NE description \"%s\"" s) ()

  let shift delta t = {t with
                       first = Id_with_proj.shift delta t.first;
                       items = Id_with_proj_set.map (fun id -> Id_with_proj.shift delta id) t.items;
                      }
end

(* ======================================================================================================================== *)
module Conll = struct

  module Id = Conll_types.Id

  (* ------------------------------------------------------------------------ *)
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

  let root = { line_num = -1; id=(0,None); form="ROOT"; lemma="__"; upos="_X"; xpos=""; feats=[]; deps=[]; efs=[] }

  type prefix = No | S | I | D | E
  let get_prefix label =
    if String.length label < 2
    then No
    else match String.sub label 0 2 with
      | "S:" -> S | "I:" -> I | "D:" -> D | "E:" -> E
      | _ -> No

  let compare_pref pref1 pref2 =
    if List.mem pref1 [No; S] && List.mem pref2 [I; D; E]
    then -1
    else
    if List.mem pref1 [I; D; E] && List.mem pref2 [No; S]
    then 1
    else 0


  let compare_dep (id1,lab1) (id2,lab2) =
    match compare_pref (get_prefix lab1) (get_prefix lab2) with
    | 0 ->
      begin
        match Id.compare id1 id2 with
        | 0 -> Stdlib.compare lab1 lab2
        | x -> x
      end
    | x -> x

  let build_line ~id ~form ?(lemma="_") ?(upos="_") ?(xpos="_") ?(feats=[]) ?(deps=[]) () =
    { line_num = -1; id; form; lemma; upos; xpos; feats; deps=List.sort compare_dep deps; efs=[] }

  let compare l1 l2 =
    match (l1.id, l2.id) with
    | ((id1, _), (id2, _)) when id1 <> id2 -> Stdlib.compare id1 id2
    | ((_,None), (_,Some _)) -> -1
    | ((_,Some _), (_,None)) -> 1
    | ((_,Some sub_id1), (_,Some sub_id2)) -> Stdlib.compare sub_id1 sub_id2
    | ((id,None), (_,None)) ->
      error ~line:l2.line_num ~fct:"Conll.compare" ~msg:(sprintf "identifier \"%d\" already used " id) ()

  (* deal with UD features like "Number[psor]" written "Number__psor" in Grew to void clashes with Grew brackets usage *)
  let encode_feat_name s = Str.global_replace (Str.regexp "\\[\\([0-9a-z]+\\)\\]") "__\\1" s
  let decode_feat_name s = Str.global_replace (Str.regexp "__\\([0-9a-z]+\\)$") "[\\1]" s

  let fs_to_string = function
    | [] -> "_"
    | list -> String.concat "|" (List.map (fun (f,v) -> sprintf "%s=%s" (decode_feat_name f) v) list)

  let remove_feat feat_name t =
    let new_feats =
      try List.remove_assoc feat_name t.feats
      with Not_found -> t.feats in
    { t with feats = new_feats }

  let remove_prefix prefix t =
    let new_feats =
      CCList.filter
        (fun (f,v) -> not (String_.check_prefix prefix f)
        ) t.feats in
    { t with feats = new_feats }

  let get_feat feat_name t =
    try Some (List.assoc feat_name t.feats)
    with Not_found -> None

  let filter_ud_misc t =
    CCList.filter_map
      (fun (f,v) ->
         match String_.remove_prefix "_UD_MISC_" f with
         | Some s -> Some (s,v)
         | None -> None
      ) t.feats

  let add_feat line_num (fn,fv) feats =
    let rec loop feats = match feats with
      | [] -> [(fn,fv)]
      | (hfn, _) :: tail when fn < hfn -> (fn,fv) :: feats
      | (hfn, hfv) :: tail when fn > hfn -> (hfn, hfv) :: (loop tail)
      | (_, hfv) :: tail when hfv = fv ->
        Log.fwarning "[line %d], feature %s=%s is defined twice" line_num fn fv;
        (fn,fv) :: tail
      | (_, hfv) :: _ -> error ~line:line_num ~msg:(sprintf "inconsistent features: %s id defined twice with two different values" fn) () in
    loop feats

  let add_feat_line (fn,fv) line = { line with feats = add_feat line.line_num (fn,fv) line.feats }

  let add_feat_lines conll_id (fn,fv) lines =
    List.map
      (fun line ->
         if line.id = conll_id
         then add_feat_line (fn,fv) line
         else line
      )  lines

  let is_extended (_,lab) = String_.check_prefix "E:" lab

  let string_of_ext = function
    | [] -> "_"
    | ext -> String.concat "|" (
        List.map (fun (g,l) ->
            sprintf "%s:%s" (Id.to_string g) (String.sub l 2 ((String.length l)-2))
          ) ext
      )

  let check_line line = ()
  (* match (line.id, get_feat "_UD_empty" line) with
     | ((_,None), None) -> ()
     | ((_,Some _), Some _) -> ()
     | ((_,None), Some _) -> error ~fct:"Conll.check_line" ~line:line.line_num "inconsistent emptyness: empty node and non empty identifier";
     | ((_,Some _), None) -> error ~fct:"Conll.check_line" ~line:line.line_num "inconsistent emptyness: empty identifier and non empty node" *)

  let line_to_string ~cupt mwe_line l =

    (* Strange: the function Id_map.find does not work correctly in this context: maybe a order problem... *)
    (* TODO: explore the problem *)
    let assoc_list = Id_map.fold (fun k v acc -> (k,v)::acc) mwe_line [] in

    check_line l;
    let mwe_info =
      try String.concat ";" (List.sort Stdlib.compare (List.assoc l.id assoc_list))
      with Not_found -> "*" in

    let (ext,not_ext) = List.partition is_extended l.deps in
    let (gov_list, lab_list) = List.split not_ext in
    sprintf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%s"
      (Id.to_string l.id)
      l.form
      l.lemma
      l.upos
      l.xpos
      (fs_to_string l.feats)
      (match gov_list with [] -> "_" | l -> List_.to_string Id.to_string "|" l)
      (match lab_list with [] -> "_" | l -> String.concat "|" l)
      (string_of_ext ext)
      (fs_to_string l.efs)
      (if cupt then "\t" ^ mwe_info else "")

  (* ------------------------------------------------------------------------ *)
  type multiword = {
    mw_line_num: int option;
    first: int;
    last: int;
    fusion: string;
    mw_efs: (string * string) list;
  }

  let mw_equals t1 t2 = t1.first = t2.first && t1.last = t2.last && t1.fusion = t2.fusion

  let multiword_to_string ~cupt l =
    sprintf "%d-%d\t%s\t_\t_\t_\t_\t_\t_\t_\t%s%s"
      l.first
      l.last
      l.fusion
      (fs_to_string l.mw_efs)
      (if cupt then "\t*" else "")

  (* ------------------------------------------------------------------------ *)
  type t = {
    file: string option;
    meta: string list;
    lines: line list;
    multiwords: multiword list;
    mwes: Mwe.t Int_map.t;
  }

  let void = {file=None; meta=[]; lines=[]; multiwords=[]; mwes=Int_map.empty}
  let is_void x = (x = void)

  let sof = function
    | Some f -> sprintf "File %s, " f
    | None -> ""

  let token_size t = List.length t.lines

  let get_line_num t =
    match t.lines with
    | [] -> error ~msg:"empty" ()
    | first :: _ -> first.line_num

  let check t =
    (* check consecutive ids *)
    let rec loop = function
      | [] | [_] -> ()
      | {id=(i1,None)}::({id=(i2,None);line_num}::_ as tail) ->
        if i2 <> i1 + 1
        then Log.fwarning "[Conll, %sline %d], idenfier %d is different from expected %d" (sof t.file) line_num i2 (i1+1);
        loop tail
      | {id=(i1,Some _)}::({id=(i2,None);line_num}::_ as tail) ->
        if i2 <> i1 + 1
        then Log.fwarning "[Conll, %sline %d], idenfier %d is different from expected %d" (sof t.file) line_num i2 (i1+1);
        loop tail
      | {id=(i1,None)}::({id=(i2,Some e2);line_num}::_ as tail) ->
        if i1 <> i2
        then Log.fwarning "[Conll, %sline %d], idenfier %d.%d is different from expected %d.1" (sof t.file) line_num i2 e2 i1
        else if e2 <> 1
        then Log.fwarning "[Conll, %sline %d], idenfier %d.%d is different from expected %d.1" (sof t.file) line_num i2 e2 i1;
        loop tail
      | {id=(i1,Some e1)}::({id=(i2,Some e2);line_num}::_ as tail) ->
        if (i1 <> i2) || (e2 <> e1+1)
        then Log.fwarning "[Conll, %sline %d], idenfier %d.%d is different from expected %d.%d" (sof t.file) line_num i2 e2 i1 (e1+1);
        loop tail
    in loop t.lines;

    (* check dependency and loops *)
    let id_list = List.map (fun {id} -> id) t.lines in
    List.iter (
      fun {id; line_num; deps} ->
        List.iter (
          fun (i,_) ->
            if (fst i)>0 && not (List.mem i id_list)
            then error ?file:t.file ~line:line_num ~msg:(sprintf "cannot find gov identifier %s" (Id.to_string i)) ();
            if id = i
            then error ?file:t.file ~line:line_num ~msg:(sprintf "loop in dependency %s" (Id.to_string i)) ();
        ) deps
    ) t.lines

  let empty file = { file;  meta=[]; lines=[]; multiwords=[]; mwes=Int_map.empty; }

  (* ========== dealing with sentid information ========== *)
  let rec get_sentid_from_meta = function
    | [] -> None
    | line::tail ->
      match Str.full_split (Str.regexp "# ?sent_?id ?[:=]?[ \t]?") line with
      | [Str.Delim _; Str.Text t] -> Some t
      | _ ->
        match Str.bounded_split (Str.regexp " ") line 4 with
        (* deal with sent_id declaration of the PARSEME project *)
        | ["#"; "source_sent_id"; "="; id ] -> Some id
        | _ -> get_sentid_from_meta tail

  let get_sentid_meta t = get_sentid_from_meta t.meta

  let rm_sentid_meta t =
    List.fold_left (fun acc line ->
        match Str.full_split (Str.regexp "# ?sent_?id ?[:=]?[ \t]?") line with
        | [Str.Delim _; Str.Text t] -> acc
        | _ -> line :: acc
      ) [] t

  let get_sentid_feats t =
    match t.lines with
    | [] -> None
    | head::_ ->
      match get_feat "sent_id" head
      with None -> get_feat "sentid" head | x -> x

  let get_sentid t =
    match (get_sentid_meta t, get_sentid_feats t) with
    | (None, None) -> None
    | (Some id, None) -> Some id
    | (None, Some id) -> Some id
    | (Some idm, Some idf) when idm = idf ->
      Log.fwarning "[Conll, %ssentid %s], sentid declared both in meta and feats" (sof t.file) idm;
      Some idm
    | (Some idm, Some idf) ->
      error ?file:t.file ~msg:(sprintf "unconsistent sentid (\"%s\" in meta VS \"%s\" in feats)" idm idf) ()

  let remove_sentid_feats = function
    | [] -> []
    | head::tail -> (head |> remove_feat "sentid" |> remove_feat "sent_id") :: tail

  let ensure_sentid_in_meta ?default t =
    match (get_sentid_meta t, get_sentid_feats t, default) with
    | (None, None, None) ->
      Log.fwarning "[Conll.ensure_sentid_in_meta%s, line %d] sentence without sentid"
        (match t.file with None -> "" | Some f -> ", file "^f)
        (get_line_num t); t
    | (None, None, Some def) -> { t with meta = (sprintf "# sent_id = %s" def) :: t.meta; }
    | (Some id, _,_) -> t
    | (None, Some id,_) ->
      { t with
        meta = (sprintf "# sent_id = %s" id) :: t.meta;
        lines = remove_sentid_feats t.lines }

  let set_sentid new_sentid t =
    let meta_line = sprintf "# sent_id = %s" new_sentid in
    let rec loop = function
      | [] -> [meta_line]
      | line::tail ->
        match Str.full_split (Str.regexp "# ?sent_?id ?[:=]?[ \t]?") line with
        | [Str.Delim _; Str.Text t] -> meta_line :: tail
        | _ -> line :: (loop tail) in
    { t with meta = loop t.meta}

  (* ---------- dealing with sentid information ---------- *)







  let intern_parse_feats ~file line_num = function
    | "_" -> []
    | feats ->
      List.fold_left
        (fun acc feat ->
           match Str.bounded_split (Str.regexp "=") feat 2 with
           | [feat_name] -> add_feat line_num (encode_feat_name feat_name, "true") acc
           | [feat_name; feat_value] -> add_feat line_num (encode_feat_name feat_name, feat_value) acc
           | _ -> error ?file ~line:line_num ~fct:"Conll.parse_feats" ~msg:(sprintf "failed to parse \"%s\"" feats) ()
        ) [] (Str.split (Str.regexp "|") feats)

  let parse_feats ~file line_num s =
    try intern_parse_feats ~file line_num s
    with _ ->
      let new_s = Str.global_replace (Str.regexp "=|") "=__PIPE__" s in
      List.map
        (fun (f,v) -> (f, Str.global_replace (Str.regexp "__PIPE__") "|" v))
        (intern_parse_feats ~file line_num new_s)

  let sort_feats feats =
    List.sort
      (fun (id1,_) (id2,_) -> Stdlib.compare (String.lowercase_ascii id1) (String.lowercase_ascii id2))
      feats

  let underscore s = if s = "" then "_" else s

  let set_label id new_label t =
    { t with lines = List.map
                 (fun line ->
                    if line.id=id
                    then match line.deps with
                      | [(gov,lab)] -> {line with deps=[(gov,new_label)]}
                      | _ -> error ~msg:"ambiguous set_label" ()
                    else line
                 ) t.lines
    }

  (* parsing of extended deps encoded in column 9 in UD *)
  let parse_extended_deps = function
    | "_" -> []
    | s ->
      let sd_list = Str.split (Str.regexp "|") s in
      CCList.filter_map (
        fun sd -> match Str.bounded_split (Str.regexp ":") sd 2 with
          | [gov;lab] -> Some (Id.of_string gov, "E:"^lab)  (* E: is the prefix for extended relations *)
          | [_] -> None
          | _ -> error ~msg:(sprintf "cannot parse extended dependency \"%s\"" sd) ()
      ) sd_list

  let add_feat_id id (fn, fv) t =
    let new_lines = List.map (fun l -> if l.id = id then add_feat_line (fn, fv) l else l) t.lines in
    {t with lines = new_lines}

  let add_mw_feats t =
    List.fold_left
      (fun acc {first; last; fusion; mw_efs} ->
         acc
         |> (add_feat_id (first,None) ("_UD_mw_fusion", fusion))
         |> (add_feat_id (first,None) ("_UD_mw_span", string_of_int (last-first+1)))
         |> (List.fold_right (fun (f,v) -> add_feat_id (first,None) ("_UD_MISC_"^f,v)) mw_efs)
      ) t t.multiwords

  (* move MISC feats in feats with prefix _MISC_ *)
  let line_misc_to_feats ?(prefix="") l =
    let new_feats =
      List.fold_left
        (fun acc (n,v) -> (prefix^n,v)::acc)
        l.feats l.efs in
    { l with feats = new_feats; efs=[] }

  let misc_to_feats ?prefix t =
    { t with lines = List.map (line_misc_to_feats ?prefix) t.lines }

  (* move feats with prefix _MISC_ in MISC feats *)
  let line_feats_to_misc l =
    let (pre_efs, feats) =
      List.partition (
        fun (n,v) -> String.length n > 6 && String.sub n 0 6 = "_MISC_"
      ) l.feats in
    let new_efs = List.map (fun (n,v) -> (String.sub n 6 ((String.length n) - 6),v)) pre_efs in
    let efs = List.sort Stdlib.compare (new_efs @ l.efs) in
    { l with efs; feats }

  let feats_to_misc t = { t with lines = List.map line_feats_to_misc t.lines }

  (* move efs into feats (without prefix) *)
  let line_tf_wf l =
    let feats_with_misc =
      List.fold_left
        (fun acc (n,v) -> match n with
           | "SpaceAfter" -> acc
           | _ when List.mem_assoc n acc -> error ~msg:"feature already used" ()
           | _ -> (n,v)::acc
        ) l.feats l.efs in
    { l with feats = feats_with_misc; efs=[] }

  let copy_form tar_feat line =
    if List.mem_assoc tar_feat line.feats
    then line
    else { line with feats = (tar_feat, line.form) :: line.feats }

  let add_tf_wf_empty_node line =
    match line.id with
    | (_,None) -> line
    | _ -> line
           |> add_feat_line ("textform", "_")
           |> add_feat_line ("wordform", "_")

  let add_tf_wf t =
    (* first step: add textform on amalgams *)
    let with_mwt =
      List.fold_left
        (fun acc {first; last; fusion } ->
           let rec loop index acc2 =
             if index > last
             then acc2
             else loop (index+1) (add_feat_id (index,None) ("textform", "_") acc2)
           in loop (first+1) (add_feat_id (first,None) ("textform", fusion) acc)
        ) t t.multiwords in
    { with_mwt with
      lines =
        with_mwt.lines
        |> List.map add_tf_wf_empty_node    (* second step: empty node *)
        |> List.map (copy_form "textform")  (* third step: by default, copy form *)
        |> List.map (copy_form "wordform")
    }

  (*
    13 --> (13,None)
    13/2 --> (13, Some 2)
    _ --> fail
  *)
  let parse_mwe_id_proj_opt ?file ?line s =
    match List.map int_of_string_opt (Str.split (Str.regexp "/") s) with
    | [Some i] -> (i, None)
    | [Some i; Some j] -> (i, Some j)
    | _ -> error ?file ?line ~msg:(sprintf "Cannot parse mwe_id \"%s\"" s) ()

  let add_mwe_nodes ?file lines conll =
    let (new_lines, mwes) = List.fold_left
        (fun (acc_lines, acc) (line_num, conll_id, mwe_field) ->
           let items = Str.split (Str.regexp ";") mwe_field in
           List.fold_left
             (fun (acc2_lines, acc2) item ->
                match Str.split (Str.regexp ":") item with
                | [] -> error ?file ~line:line_num ~msg:(sprintf "Cannot parse mwe item \"%s\"" item) ()
                | mwe_id_string::tail ->
                  let (mwe_id, proj_opt) = parse_mwe_id_proj_opt ?file ~line:line_num mwe_id_string in

                  let new_lines =
                    (* match snd mwe_id with
                       | None -> acc2_lines
                       | Some i -> add_feat_lines conll_id ("_MWE_partial_projection", (string_of_int i)) *)
                    acc2_lines in

                  (* /i *)

                  match tail with
                  | [desc] -> let mwe = Mwe.parse conll_id proj_opt desc in
                    (new_lines, Int_map.add mwe_id mwe acc2)
                  | [] ->
                    begin
                      match Int_map.find_opt mwe_id acc2 with
                      | None -> error ?file ~line:line_num ~msg:(sprintf "Cannot find definition for mwe_id: \"%d\"" mwe_id) ()
                      | Some x -> (new_lines, Int_map.add mwe_id {x with items = Id_with_proj_set.add (conll_id, proj_opt) x.items} acc2)
                    end
                  | _ -> error ?file ~line:line_num ~msg:(sprintf "Cannot parse mwe item \"%s\"" item) ()
             ) (acc_lines, acc) items
        ) (conll.lines, Int_map.empty) lines in
    { conll with lines = new_lines; mwes }

  exception Empty_conll
  (* parse a list of line corresponding to one conll structure *)
  let parse_rev ?(tf_wf=false) ?file lines_rev =
    let (conll,mwe) = (* mwe contains the list of (line_num, id, mwe_field) to be processed later in add_mwe_nodes *)
      List.fold_left
        (fun (acc, acc_mwe) (line_num, line) ->
           match line with
           | "" -> (acc, acc_mwe)
           | _ when line.[0] = '#' -> ({ acc with meta = line :: acc.meta }, acc_mwe)
           | _ ->
             match Str.split (Str.regexp "\t") line with
             | (f1 :: form :: lemma :: upos :: xpos :: feats :: govs :: dep_labs :: c9 :: c10 :: tail) as l ->
               begin
                 try
                   match Str.split (Str.regexp "-") f1 with
                   | [f;l] -> ({acc with multiwords = {
                       mw_line_num = Some line_num;
                       first=int_of_string f;
                       last=int_of_string l;
                       fusion=form;
                       mw_efs= sort_feats (parse_feats ~file line_num c10);
                     } :: acc.multiwords
                     }, acc_mwe)
                   | [string_id] ->
                     let gov_list = match govs with
                       | "_" | "-" -> []
                       | _ -> List.map Id.of_string (Str.split (Str.regexp "|") govs) in
                     let lab_list = match dep_labs with
                       | "_" | "-" -> []
                       | _ -> Str.split (Str.regexp "|") dep_labs in
                     let prim_deps = match (gov_list, lab_list) with
                       | ([(0,None)], []) -> [] (* handle Talismane output on tokens without gov *)
                       | _ ->
                         try List.combine gov_list lab_list
                         with Invalid_argument _ -> error ?file ~line:line_num ~msg:"inconsistent relation specification" () in

                     let deps = match c9 with
                       | "_" -> prim_deps
                       | _ -> prim_deps @ (parse_extended_deps c9) in

                     let id = Id.of_string (string_id) in
                     let new_line =
                       let feats =
                         match id with
                         | (_,None) -> parse_feats ~file line_num feats
                         | _ -> ("_UD_empty", "Yes") :: (parse_feats ~file line_num feats) in
                       let orfeo_feats = match tail with
                         | [start; stop; speaker] -> [("_start",start); ("_stop",stop); ("_speaker",speaker); ]
                         | _ -> [] in
                       let efs = sort_feats (parse_feats ~file line_num c10) in
                       {
                         line_num;
                         id;
                         form = underscore form;
                         lemma = underscore lemma;
                         upos = underscore upos;
                         xpos =
                           if orfeo_feats = []
                           then underscore xpos
                           else "_"; (* Hack to hide xpos in Orfeo (always upos=xpos) *)
                         feats = sort_feats (feats @ orfeo_feats);
                         deps;
                         efs;
                       } in

                     let new_acc_mwe = match tail with
                       | [] | ["*"] | [_;_;_]-> acc_mwe
                       | [x] -> (line_num, id, x) :: acc_mwe
                       | _ -> error ?file ~line:line_num ~data:line ~msg:(sprintf "illegal line, %d fields (10, 11 or 13 are expected)" (List.length l)) ()
                     in

                     ({acc with lines = new_line :: acc.lines }, new_acc_mwe)
                   | _ -> error ?file ~line:line_num ~msg:(sprintf "illegal field one \"%s\"" f1) ()
                 with
                 | Id.Wrong_id id -> error ?file ~line:line_num ?sent_id:(get_sentid_from_meta acc.meta) ~msg:(sprintf "illegal identifier \"%s\"" id) ()
                 | Conll_error json -> error ~data:line ?sent_id:(get_sentid_from_meta acc.meta) ~prev:json ()
                 | exc -> error ?file ~line:line_num ~data:line ~msg:(sprintf "unexpected exception \"%s\"" (Printexc.to_string exc)) ()
               end
             | l -> error ?file ~line:line_num ~data:line ~msg:(sprintf "illegal line, %d fields (10, 11 or 13 are expected)" (List.length l)) ()
        ) (empty file,[]) lines_rev in
    if List.length conll.lines = 0 then raise Empty_conll;
    check conll;
    if tf_wf
    then
      conll
      |> misc_to_feats
      |> add_tf_wf
      |> (add_mwe_nodes ?file mwe)
    else
      conll
      |> (misc_to_feats ~prefix: "_MISC_") (* add features _MISC_ *)
      |> add_mw_feats  (* add features _UD_ *)
      |> (add_mwe_nodes ?file mwe)

  let parse ?tf_wf ?file lines = parse_rev ?tf_wf ?file (List.rev lines)

  let from_string s =
    let lines = Str.split (Str.regexp "\n") s in
    let num_lines = List.mapi (fun i l -> (i+1,l)) lines in
    parse num_lines

  (* load conll structure from file: the file must contain only one structure *)
  let load ?tf_wf file =
    let lines_rev = File.read_rev file in
    parse_rev ?tf_wf ~file lines_rev

  let to_string ?(cupt=false) t =
    let t = feats_to_misc t in
    let buff = Buffer.create 32 in
    List.iter (bprintf buff "%s\n") t.meta;

    let mwe_line =
      Int_map.fold
        (fun mwe_id mwe acc ->
           let (id_first, proj_first) = mwe.Mwe.first in
           let old_ = try Id_map.find id_first acc with Not_found -> [] in
           let new_ = (sprintf "%d%s:%s"
                         mwe_id
                         (match proj_first with None -> "" | Some i -> sprintf "/%d" i)
                         (Mwe.to_string mwe)
                      ) :: old_ in
           let acc_tmp = Id_map.add id_first new_ acc in
           Id_with_proj_set.fold
             (fun (conll_id, proj_opt) acc2 ->
                let old_ = try Id_map.find conll_id acc2 with Not_found -> [] in
                let new_ = (sprintf "%d%s" mwe_id (match proj_opt with None -> "" | Some i -> sprintf "/%d" i)) :: old_ in
                Id_map.add conll_id new_ acc2
             ) mwe.Mwe.items acc_tmp
        ) t.mwes Id_map.empty in

    let rec loop (lines, multiwords) = match (lines, multiwords) with
      | ([],[]) -> ()
      | (line::tail,[]) ->
        bprintf buff "%s\n" (line_to_string ~cupt mwe_line line); loop (tail,[])
      | (line::tail, {first}::_) when (fst line.id) < first ->
        bprintf buff "%s\n" (line_to_string ~cupt mwe_line line); loop (tail,multiwords)
      | (_, mw::tail) ->
        bprintf buff "%s\n" (multiword_to_string ~cupt mw); loop (lines,tail) in
    loop (t.lines, t.multiwords);
    Buffer.contents buff

  let node_to_dot_label buff line =
    bprintf buff "[label= <<TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\">\n";
    begin
      match line.form with
      | "_" -> ()
      | form -> bprintf buff "<TR><TD COLSPAN=\"3\"><B>%s</B></TD></TR>\n" form
    end;
    begin
      match line.lemma with
      | "_" -> ()
      | lemma -> bprintf buff "<TR><TD COLSPAN=\"3\"><B>%s</B></TD></TR>\n" lemma
    end;
    begin
      match line.upos with
      | "_" -> ()
      | upos -> bprintf buff "<TR><TD COLSPAN=\"3\"><B>%s</B></TD></TR>\n" upos
    end;
    List.iter (fun (f,v) ->
        bprintf buff "<TR><TD ALIGN=\"right\">%s</TD><TD>=</TD><TD ALIGN=\"left\">%s</TD></TR>\n" f v
      ) line.feats;
    bprintf buff "</TABLE>> ];\n"

  let to_dot t =
    let buff = Buffer.create 32 in
    bprintf buff "digraph G {\n";
    bprintf buff "  node [shape=Mrecord];\n";
    List.iter
      (fun line ->
         bprintf buff "  N_%s " (Id.to_dot line.id);
         node_to_dot_label buff line;
         List.iter (fun (gov, lab) ->
             match lab with
             (* in phrase structure tree, no reals label on edges *)
             | "__SUB__" -> bprintf buff "  N_%s -> N_%s;\n" (Id.to_dot gov) (Id.to_dot line.id)
             | _ -> bprintf buff "  N_%s -> N_%s [label=\"%s\"];\n" (Id.to_dot gov) (Id.to_dot line.id) lab
           ) line.deps
      ) t.lines;
    bprintf buff "}\n";
    Buffer.contents buff

  let save_dot output_file t =
    let in_ch = open_out output_file in
    fprintf in_ch "%s" (to_dot t);
    close_out in_ch




  (* ========== adding multiwords lines from features ========== *)
  let insert_multiword id span fusion mw_efs multiwords =
    let item = { mw_line_num=None; first=id; last= id+span-1; fusion; mw_efs } in
    let rec loop = function
      | [] -> [item]
      | h::t when mw_equals h item -> h::t
      | h::t when id < h.first -> item :: t
      | h::t -> h::(loop t) in
    loop multiwords

  let normalize_multiwords t =
    let new_multiwords = List.fold_left
        (fun acc line ->
           let ud_misc = filter_ud_misc line in
           match (get_feat "_UD_mw_fusion" line, get_feat "_UD_mw_span" line, ud_misc) with
           | (None, None, []) -> acc
           | (None, None, _) -> Log.warning "features _UD_MISC_* cannot be interpreted here"; acc
           | (Some fusion, Some string_span, efs) ->
             let span =
               try int_of_string string_span
               with Failure _ -> error ?file:t.file ~line:(get_line_num t) ~msg:"_UD_mw_span must be integer" () in
             insert_multiword (fst line.id) span fusion efs acc
           | _ -> error ?file:t.file ~line:(get_line_num t) ~msg:"inconsistent mw specification" ()
        )
        t.multiwords t.lines in
    { t with
      multiwords = new_multiwords;
      lines = List.map (remove_prefix "_UD_") t.lines
    }
  (* ---------- adding multiwords lines from features ---------- *)



  (* ========== retrieving or building full text on a sentence ========== *)
  let concat_words words = Sentence.fr_clean_spaces (String.concat "" words)

  let final_space line =
    try if List.assoc "SpaceAfter" line.efs = "No" then "" else " "
    with Not_found -> " "

  let mw_final_space mw =
    try if List.assoc "SpaceAfter" mw.mw_efs = "No" then "" else " "
    with Not_found -> " "

  let build highlight_fct t =
    let rec loop = function
      | ([],[]) -> []
      (* skip empty words *)
      | (line::tail, mw) when snd line.id <> None -> loop (tail, mw)
      | ([line],[]) ->
        let text = highlight_fct (fst line.id, fst line.id) line.form in
        [text]
      | (line::tail,[]) ->
        let text = highlight_fct (fst line.id, fst line.id) line.form in
        (text ^ (final_space line)) :: (loop (tail,[]))
      | (line::tail, ((mw::_) as multiwords)) when (fst line.id) < mw.first ->
        let text = highlight_fct (fst line.id, fst line.id) line.form in
        (text ^ (final_space line)) :: (loop (tail,multiwords))
      | (line::tail, ((mw::_) as multiwords)) when (fst line.id) = mw.first ->
        let text = highlight_fct (mw.first, mw.last) mw.fusion in
        (text ^(mw_final_space mw)) :: (loop (tail,multiwords))
      | (line::tail, (mw::mw_tail)) when (fst line.id) > mw.last ->
        (loop (line::tail,mw_tail))
      | (_::tail, multiwords) ->
        (loop (tail,multiwords))
      | (_, mw::_) ->
        error ?file:t.file ?line:mw.mw_line_num ~msg:"Inconsistent multiwords" () in
    let form_list = loop (t.lines, t.multiwords) in
    concat_words form_list

  let build_sentence t = build (fun _ x -> x) t

  let html_sentence ?(highlight=[]) t =
    build (fun (first,last) x ->
        if List.exists (fun id -> first <= id && id <= last) highlight
        then sprintf "<span class=\"highlight\">%s</span>" x
        else x
      ) t

  let text_regexp = Str.regexp "# ?\\(sentence-\\)?text ?[:=]?[ \t]?"

  let get_sentence {meta; lines} =
    let rec loop = function
      | [] -> None
      | line::tail ->
        match Str.full_split text_regexp line with
        | [Str.Delim _; Str.Text t] -> Some t
        | _ -> loop tail in
    loop meta

  let rm_sentence_text t =
    List.fold_left (fun acc line ->
        match Str.full_split text_regexp line with
        | [Str.Delim _; Str.Text t] -> acc
        | _ -> line :: acc
      ) [] t

  (* ---------- retrieving or building full text on a sentence ---------- *)

  let web_anno t =
    let buff = Buffer.create 32 in
    List.iter
      (fun line ->
         match line.deps with

         (* UD_French-GSD / UD_French-Sequoia *)
         | [] when line.xpos ="_" ->
           bprintf buff "%s\t%s\t_\t_\t%s\t_\t_\t_\t_\t_\n" (Id.to_string line.id) line.form line.upos
         | [(gov_id,label)] when line.xpos ="_" ->
           bprintf buff "%s\t%s\t_\t_\t%s\t_\t%s\t%s\t_\t_\n"
             (Id.to_string line.id)
             line.form
             line.upos
             (Id.to_string gov_id) label


         | [] ->
           let xpos = match line.xpos with "PONCT" -> "_" | x -> x in
           bprintf buff "%s\t%s\t_\t%s\t%s\t_\t_\t_\t_\t_\n" (Id.to_string line.id) line.form xpos xpos
         | [(gov_id,"ponct")] ->
           bprintf buff "%s\t%s\t_\t_\t_\t_\t_\t_\t_\t_\n" (Id.to_string line.id) line.form
         | [(gov_id,label)] ->
           bprintf buff "%s\t%s\t_\t%s\t%s\t_\t%s\t%s\t_\t_\n"
             (Id.to_string line.id)
             line.form
             line.xpos line.xpos
             (Id.to_string gov_id) label
         | _ -> error ~line:line.line_num ~msg:"multiple deps not handled" ()
      ) t.lines;
    Buffer.contents buff

  let merge_mwes delta mwe1 mwe2 =
    match Int_map.max_binding_opt mwe1 with
    | None -> mwe2
    | Some (largest,_) ->
      Int_map.fold
        (fun k mwe acc ->
           Int_map.add (k+largest) (Mwe.shift delta mwe) acc
        ) mwe2 mwe1

  let shift delta t =
    let new_lines = List.map
        (fun line -> {line with
                      id=Id.shift delta line.id;
                      deps = List.map (function
                          | ((0,None),"root") -> ((0,None),"root")
                          | (id,label) -> (Id.shift delta id, label)
                        ) line.deps;
                     }) t.lines in
    let new_multiwords = List.map
        (fun multiword -> {multiword with
                           mw_line_num = None;
                           first = multiword.first + delta;
                           last = multiword.last + delta;
                          }) t.multiwords in
    {t with lines = new_lines; multiwords = new_multiwords }

  let merge new_sentid t1 t2 =
    let new_text = match (get_sentence t1, get_sentence t2) with
      | Some s1, Some s2 -> Sentence.fr_clean_spaces (s1 ^ " " ^ s2)
      | _ -> error ~msg:"cannot merge without sentence" () in
    let new_meta =
      (sprintf "# sent_id = %s" new_sentid) ::
      (sprintf "# text = %s" new_text) ::
      (t1.meta |> rm_sentid_meta |> rm_sentence_text) in
    match CCList.last_opt t1.lines with
    | None -> error ~msg:"Empty t1 in merge" ()
    | Some { id = (delta,_) } ->
      let shifted_t2 = shift delta t2 in
      {t1 with
       meta = new_meta;
       lines = t1.lines @ shifted_t2.lines;
       multiwords = t1.multiwords @ shifted_t2.multiwords;
       mwes = merge_mwes delta t1.mwes t2.mwes;
      }

end (* module Conll *)



(* ======================================================================================================================== *)
module Conll_corpus = struct
  type t = (string * Conll.t) array

  let cpt = ref 0
  let res = ref []

  let reset () = cpt := 0; res := []

  let add_lines ?tf_wf ?log_file file lines =

    let rev_locals = ref [] in
    let save_one () =
      begin
        try
          let conll = Conll.parse_rev ?tf_wf ~file !rev_locals in
          incr cpt;
          let base = Filename.basename file in
          let sentid = match Conll.get_sentid conll with Some id -> id | None -> sprintf "%s_%05d" base !cpt in
          res := (sentid,conll) :: !res
        with
        | Conll.Empty_conll -> ()
        | (Conll_error json) as e ->
          begin
            match log_file with
            | None -> raise e
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
      List.iter
        (fun (line_num,line) -> match line with
           | "" when !rev_locals = [] -> Log.fwarning "[Conll, File %s] Several blank lines around line %d" file line_num;
           | "" -> save_one ()
           | _ -> rev_locals := (line_num,line) :: !rev_locals
        ) lines in

    if !rev_locals != []
    then (
      Log.fwarning "[Conll, File %s] No blank line at the end of the file" file;
      save_one ()
    )

  let load_list ?tf_wf ?log_file file_list =
    reset ();
    List.iter (fun file -> add_lines ?tf_wf ?log_file file (File.read file)) file_list;
    Array.of_list (List.rev !res)

  let load ?tf_wf ?log_file file = load_list ?tf_wf ?log_file [file]

  let from_lines ?tf_wf ?log_file ?(basename="noname") lines =
    reset ();
    add_lines ?tf_wf ?log_file basename lines;
    Array.of_list (List.rev !res)

  let prepare_for_output conll =
    conll
    |> Conll.normalize_multiwords
    |> Conll.feats_to_misc
    |> Conll.to_string

  let save file_name t =
    let out_ch = open_out file_name in
    Array.iter (fun (id,conll) ->
        fprintf out_ch "%s" (prepare_for_output conll);
        if not (Conll.is_void conll)
        then fprintf out_ch "\n"
      ) t;
    close_out out_ch

  let save_sub file_name first last t =
    let out_ch = open_out file_name in
    for i = first to last do
      let (id,conll) = t.(i) in
      fprintf out_ch "%s" (prepare_for_output conll);
      if not (Conll.is_void conll) then fprintf out_ch "\n"
    done;
    close_out out_ch

  let dump t =
    Array.iter (fun (_,conll) ->
        printf "%s\n" (prepare_for_output conll)
      ) t

  let token_size t =
    Array.fold_left (fun acc (_,conll) -> acc + (Conll.token_size conll)) 0 t

  let web_anno corpus base_output size =
    let out_ch = ref None in
    let close () = match !out_ch with Some oc -> close_out oc; out_ch := None | _ -> () in

    Array.iteri
      (fun i (_,conll) ->
         if i mod size = 0
         then
           begin
             close ();
             out_ch := Some (open_out (sprintf "%s_%02d.conll" base_output (i/size+1)))
           end;
         match !out_ch with
         | None -> failwith "BUG web_anno"
         | Some oc -> fprintf oc "%s\n" (Conll.web_anno conll)
      ) corpus;
    close ()

  exception Found of Conll.t
  let get id corpus =
    try Array.iter (fun (i,c) -> if i=id then raise (Found c)) corpus; None
    with Found c -> Some c
end

(* ======================================================================================================================== *)
module Stat = struct
  module String_map = Map.Make (String)
  module String_set = Set.Make (String)

  type key = Upos | Xpos

  type t = {
    map: ((int String_map.t) String_map.t) String_map.t;   (* keys are label --> gov --> dep *)
    labels: String_set.t;
    tags: String_set.t;
    key: key;
  }

  let empty = { map=String_map.empty; labels=String_set.empty; tags=String_set.empty; key=Upos }


  let add3 dep stat3 =
    let old = try String_map.find dep stat3 with Not_found -> 0 in
    String_map.add dep (old+1) stat3

  let add2 gov dep stat2 =
    let old = try String_map.find gov stat2 with Not_found -> String_map.empty in
    String_map.add gov (add3 dep old) stat2

  let add label gov dep stat =
    let old = try String_map.find label stat with Not_found -> String_map.empty in
    String_map.add label (add2 gov dep old) stat

  let add_conll key conll stat =
    let lines = conll.Conll.lines in
    List.fold_left
      (fun acc line ->
         let dep_pos = match key with Upos -> line.Conll.upos | Xpos -> line.Conll.xpos in
         let acc2 = {acc with tags = String_set.add dep_pos acc.tags} in
         List.fold_left
           (fun acc3 (gov_id, label) ->
              match gov_id with
              | (0, None) -> acc3
              | _ ->
                let gov = List.find (fun line -> line.Conll.id = gov_id) lines in
                let gov_pos = match key with Upos -> gov.Conll.upos | Xpos -> gov.Conll.xpos in
                {
                  map = add label gov_pos dep_pos acc3.map;
                  labels = String_set.add label acc3.labels;
                  tags = String_set.add dep_pos acc3.tags;
                  key
                }
           ) acc2 line.Conll.deps
      ) stat lines

  let build key corpus =
    Array.fold_left
      (fun acc (_,conll) ->
         add_conll key conll acc
      ) empty corpus

  let dump stat =
    String_map.iter
      (fun label map2 ->
         String_map.iter
           (fun gov map3 ->
              String_map.iter
                (fun dep value ->
                   Printf.printf "%s -[%s]-> %s ==> %d\n" gov label dep value
                ) map3
           ) map2
      ) stat.map

  let get stat gov label dep =
    try Some (stat.map
              |> (String_map.find label)
              |> (String_map.find gov)
              |> (String_map.find dep)
             )
    with Not_found -> None

  let get_total_gov stat label gov =
    try
      let map_dep = stat.map |> (String_map.find label) |> (String_map.find gov) in
      Some (String_map.fold (fun _ x acc -> x + acc) map_dep 0)
    with Not_found -> None

  let get_total_dep stat label dep =
    try
      let map_map_gov = stat.map |> (String_map.find label)  in
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

  let get_total stat label =
    String_map.fold (fun _ map acc1 ->
        String_map.fold (fun _ x acc2 -> x + acc2) map acc1
      ) (String_map.find label stat.map) 0

  let count_compare (tag1,count1) (tag2,count2) =
    match (count1, count2) with
    | (Some i, Some j) -> Stdlib.compare j i
    | (None, Some _) -> 1
    | (Some _, None) -> -1
    | (None, None) -> Stdlib.compare tag1 tag2

  let table buff corpus_id stat label =
    let govs = String_set.fold
        (fun gov acc -> (gov, get_total_gov stat label gov) :: acc
        ) stat.tags [] in
    let sorted_govs = List.sort count_compare govs in

    let deps = String_set.fold
        (fun dep acc -> (dep, get_total_dep stat label dep) :: acc
        ) stat.tags [] in
    let sorted_deps = List.sort count_compare deps in

    bprintf buff "							<table>\n";
    bprintf buff "								<colgroup/>\n";
    String_set.iter (fun _ -> bprintf buff "								<colgroup/>\n") stat.tags;
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
    bprintf buff "										<td class=\"total\"><a href=\"../?corpus=%s&relation=%s\" class=\"btn btn-warning\" target=\"_blank\">%d</a></td>\n" corpus_id label(get_total stat label);
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
              (match get stat gov label dep with
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

  let to_html corpus_id stat =
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
           esc esc label (get_total stat label)
      ) stat.labels;

    bprintf buff "					</ul>\n";
    bprintf buff "				</div>\n";
    bprintf buff "				<div class=\"col-sm-10\">\n";
    bprintf buff "					<div class=\"tab-content\">\n";

    String_set.iter
      (fun label ->
         bprintf buff "						<div class=\"tab-pane\" id=\"%s\">\n" (escape_dot label);
         table buff corpus_id stat label;
         bprintf buff "						</div>\n";
      ) stat.labels;

    bprintf buff "					</div>\n";
    bprintf buff "				</div>\n";
    bprintf buff "			</div>\n";
    bprintf buff "		</div>\n";
    bprintf buff "	</div>\n";
    bprintf buff "</body>\n";
    bprintf buff "</html>\n";

    Buffer.contents buff

end
