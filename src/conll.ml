open Printf
open Log

open Utils
open Conll_types
open Dep
open Svg

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

  let build_line ~id ~form ?(lemma="_") ?(upos="_") ?(xpos="_") ?(feats=[]) ?(deps=[]) () =
    { line_num = -1; id; form; lemma; upos; xpos; feats; deps=[]; efs=[] }

  let compare l1 l2 =
    match (l1.id, l2.id) with
    | ((id1, _), (id2, _)) when id1 <> id2 -> Pervasives.compare id1 id2
    | ((_,None), (_,Some _)) -> -1
    | ((_,Some _), (_,None)) -> 1
    | ((_,Some sub_id1), (_,Some sub_id2)) -> Pervasives.compare sub_id1 sub_id2
    | ((id,None), (_,None)) -> error "[Conll, lines %d and %d], twice the same indentifier \"%d\"" l1.line_num l2.line_num id

  let fs_to_string = function
    | [] -> "_"
    | list -> String.concat "|" (List.map (fun (f,v) -> sprintf "%s=%s" f v) list)

  let remove_feat feat_name t =
    let new_feats =
      try List.remove_assoc feat_name t.feats
      with Not_found -> t.feats in
    { t with feats = new_feats }

  let get_feat feat_name t =
    try Some (List.assoc feat_name t.feats)
    with Not_found -> None

  let add_feat line_num (fn,fv) feats =
    let rec loop feats = match feats with
    | [] -> [(fn,fv)]
    | (hfn, _) :: tail when fn < hfn -> (fn,fv) :: feats
    | (hfn, hfv) :: tail when fn > hfn -> (hfn, hfv) :: (loop tail)
    | (_, hfv) :: tail when hfv = fv ->
      Log.fwarning "[line %d], feature %s=%s is defined twice" line_num fn fv;
      (fn,fv) :: tail
    | (_, hfv) :: _ -> error "[line %d], inconsistent features: %s id defined twice with two different values" line_num fn in
    loop feats

  let add_feat_line (fn,fv) line = { line with feats = add_feat line.line_num (fn,fv) line.feats }

  let is_extended (_,lab) = String.length lab > 2 && String.sub lab 0 2 = "E:"

  let string_of_ext = function
  | [] -> "_"
  | ext -> String.concat "|" (List.map (fun (g,l) -> sprintf "%s:%s" (Id.to_string g) (String.sub l 2 ((String.length l)-2))) ext)

  let check_line line =
    match (line.id, get_feat "_UD_empty" line) with
    | ((_,None), None) -> ()
    | ((_,Some _), Some _) -> ()
    | ((_,None), Some _) -> error "[line %d], inconsistent emptyness: empty node and non empty identifier" line.line_num;
    | ((_,Some _), None) -> error "[line %d], inconsistent emptyness: empty identifier and non empty node" line.line_num

  let line_to_string l =
    check_line l;
    let (ext,not_ext) = List.partition is_extended l.deps in
    let (gov_list, lab_list) = List.split not_ext in
    sprintf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
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

  (* ------------------------------------------------------------------------ *)
  type multiword = {
    mw_line_num: int option;
    first: int;
    last: int;
    fusion: string;
    mw_efs: (string * string) list;
}

  let mw_equals t1 t2 = t1.first = t2.first && t1.last = t2.last && t1.fusion = t2.fusion

  let multiword_to_string l =
    sprintf "%d-%d\t%s\t_\t_\t_\t_\t_\t_\t_\t%s" l.first l.last l.fusion (fs_to_string l.mw_efs)

  (* ------------------------------------------------------------------------ *)
  type t = {
    file: string option;
    meta: string list;
    lines: line list;
    multiwords: multiword list;
  }

  let to_dep t =
    let root = Dep_node.build (0,None) ["ROOT", 24] in
    let nodes = root ::
      (List.map
        (fun l -> Dep_node.build l.id [(l.form,24);(l.lemma,20)]
        ) t.lines
      ) in
    let init_edges = List.fold_left
        (fun acc l ->
          List.fold_left
            (fun acc2 (src,label) ->
              (Dep_edge.build src l.id label false) :: acc2
            ) acc l.deps
        ) [] t.lines in
    let final_edges = Dep.compute_shifts nodes (Dep.compute_levels init_edges) in
    { Dep.nodes = nodes; edges= final_edges; }


  let dump t =
    Svg.set_text_pad 1;
    let dep = to_dep t in
    Dep.dump dep;
    Dep.to_svg "test.svg" dep;
    ()

  let sof = function
    | Some f -> sprintf "File %s, " f
    | None -> ""

  let token_size t = List.length t.lines

  let get_line_num t =
    match t.lines with
    | [] -> ""
    | first :: _ -> sprintf "Line %d, " first.line_num

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
          then error "[Conll, %sline %d], cannot find gov identifier %s" (sof t.file) line_num (Id.to_string i);
          if id = i
          then error "[Conll, %sline %d], loop in dependency %s" (sof t.file) line_num (Id.to_string i);
      ) deps
    ) t.lines

  let empty file = { file;  meta=[]; lines=[]; multiwords=[] }

  let encode_feat_name s = Str.global_replace (Str.regexp "\\[\\([0-9a-z]+\\)\\]") "__\\1" s
  let decode_feat_name s = Str.global_replace (Str.regexp "__\\([0-9a-z]+\\)$") "[\\1]" s

  let parse_feats ~file line_num = function
    | "_" -> []
    | feats ->
      List.fold_left
        (fun acc feat ->
          match Str.split (Str.regexp "=") feat with
            | [feat_name] -> add_feat line_num (encode_feat_name feat_name, "true") acc
            | [feat_name; feat_value] -> add_feat line_num (encode_feat_name feat_name, feat_value) acc
            | _ -> error "[Conll, %sline %d], cannot parse feats \"%s\"" (sof file) line_num feats
        ) [] (Str.split (Str.regexp "|") feats)

  let underscore s = if s = "" then "_" else s

  let set_label id new_label t =
    { t with lines = List.map
      (fun line ->
        if line.id=id
        then match line.deps with
         | [(gov,lab)] -> {line with deps=[(gov,new_label)]}
         | _ -> error "ambiguous set_label"
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
        | _ -> error "[Conll], cannot parse extended dependency \"%s\"" sd
      ) sd_list

  let add_feat_id id (fn, fv) t =
    let new_lines = List.map (fun l -> if l.id = id then add_feat_line (fn, fv) l else l) t.lines in
    {t with lines = new_lines}

  let add_mw_feats t =
    List.fold_left
      (fun acc {first; last; fusion} ->
        acc
        |> (add_feat_id (first,None) ("_UD_mw_fusion", fusion))
        |> (add_feat_id (first,None) ("_UD_mw_span", string_of_int (last-first+1)))
      ) t t.multiwords

  exception Empty_conll
  (* parse a list of line corresponding to one conll structure *)
  let parse_rev ?file lines =
    let conll =
      List.fold_left
        (fun acc (line_num, line) ->
          match line with
          | "" -> acc
          | _ when line.[0] = '#' -> { acc with meta = line :: acc.meta }
          | _ ->
            match Str.split (Str.regexp "\t") line with
              | [ f1; form; lemma; upos; xpos; feats; govs; dep_labs; c9; c10 ] ->
              begin
                try
                  match Str.split (Str.regexp "-") f1 with
                  | [f;l] -> {acc with multiwords = {
                      mw_line_num = Some line_num;
                      first=int_of_string f;
                      last=int_of_string l;
                      fusion=form;
                      mw_efs= parse_feats ~file line_num c10;
                      } :: acc.multiwords
                    }
                  | [string_id] ->
                    let gov_list = if govs = "_" then [] else List.map Id.of_string (Str.split (Str.regexp "|") govs)
                    and lab_list = if dep_labs = "_" then [] else Str.split (Str.regexp "|") dep_labs in

                    let prim_deps = match (gov_list, lab_list) with
                      | ([(0,None)], []) -> [] (* handle Talismane output on tokens without gov *)
                      | _ ->
                        try List.combine gov_list lab_list
                        with Invalid_argument _ -> error "[Conll, %sline %d], inconsistent relation specification" (sof file) line_num in

                    let deps = match c9 with
                    | "_" -> prim_deps
                    | _ -> prim_deps @ (parse_extended_deps c9) in

                    let new_line =
                      let id = Id.of_string (string_id) in
                      let feats =
                        match id with
                        | (_,None) -> parse_feats ~file line_num feats
                        | _ -> ("_UD_empty", "Yes") :: (parse_feats ~file line_num feats) in
                      {
                      line_num;
                      id;
                      form = underscore form;
                      lemma = underscore lemma;
                      upos = underscore upos;
                      xpos = underscore xpos;
                      feats;
                      deps;
                      efs= parse_feats ~file line_num c10;
                      } in
                    {acc with lines = new_line :: acc.lines }
                  | _ -> error "[Conll, %sline %d], illegal field one \"%s\"" (sof file) line_num f1
                with
                | Id.Wrong_id id -> error "[Conll, %sline %d], illegal idenfier \"%s\"" (sof file) line_num id
                | Error x -> error "%s" x
                | exc -> error "[Conll, %sline %d], unexpected exception \"%s\" in line \"%s\"" (sof file) line_num (Printexc.to_string exc) line
              end
              | l -> error "[Conll, %sline %d], illegal line, %d fields (10 are expected)\n>>>>%s<<<<" (sof file) line_num (List.length l) line
        ) (empty file) lines in
      if List.length conll.lines = 0 then raise Empty_conll;
      check conll; add_mw_feats conll

  let parse ?file lines = parse_rev ?file (List.rev lines)

  let from_string s =
    let lines = Str.split (Str.regexp "\n") s in
    let num_lines = List.mapi (fun i l -> (i+1,l)) lines in
    parse num_lines

  (* load conll structure from file: the file must contain only one structure *)
  let load file =
    let lines_rev = File.read_rev file in
    parse_rev ~file lines_rev

  let to_string t =
    let buff = Buffer.create 32 in
    List.iter (bprintf buff "%s\n") t.meta;
    let rec loop (lines, multiwords) = match (lines, multiwords) with
      | ([],[]) -> ()
      | (line::tail,[]) ->
          bprintf buff "%s\n" (line_to_string line); loop (tail,[])
      | (line::tail, {first}::_) when (fst line.id) < first ->
          bprintf buff "%s\n" (line_to_string line); loop (tail,multiwords)
      | (_, mw::tail) ->
          bprintf buff "%s\n" (multiword_to_string mw); loop (lines,tail) in
    loop (t.lines, t.multiwords);
    Buffer.contents buff

  let node_to_dot_label buff line =
    bprintf buff "[label= <<TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\">\n";
    bprintf buff "<TR><TD COLSPAN=\"3\"><B>%s</B></TD></TR>\n" line.form;
    bprintf buff "<TR><TD COLSPAN=\"3\"><B>%s</B></TD></TR>\n" line.lemma;
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
          bprintf buff "  N_%s -> N_%s [label=\"%s\"];\n" (Id.to_dot gov) (Id.to_dot line.id) lab
        ) line.deps
      ) t.lines;
    bprintf buff "}\n";
    Buffer.contents buff

  let save_dot output_file t =
    let in_ch = open_out output_file in
    fprintf in_ch "%s" (to_dot t);
    close_out in_ch

  (* ========== dealing with sentid information ========== *)
  let get_sentid_meta t =
    let rec loop = function
      | [] -> None
      | line::tail ->
        match Str.full_split (Str.regexp "# ?sent_?id ?[:=]?[ \t]?") line with
        | [Str.Delim _; Str.Text t] -> Some t
        | _ -> loop tail
    in loop t.meta

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
      error "[Conll, %s], unconsistent sentid (\"%s\" in meta VS \"%s\" in feats)" (sof t.file) idm idf

  let remove_sentid_feats = function
    | [] -> []
    | head::tail -> (head |> remove_feat "sentid" |> remove_feat "sent_id") :: tail
  let ensure_sentid_in_meta t =
    match (get_sentid_meta t, get_sentid_feats t) with
    | (None, None) -> error "[Conll, %s%s], no sentid" (sof t.file) (get_line_num t)
    | (Some id, _) -> t
    | (None, Some id) ->
      { t with
        meta = (sprintf "# sent_id = %s" id) :: t.meta;
        lines = remove_sentid_feats t.lines }
  (* ---------- dealing with sentid information ---------- *)



  (* ========== adding multiwords lines from features ========== *)
  let insert_multiword id span fusion multiwords =
    let item = { mw_line_num=None; first=id; last= id+span-1; fusion; mw_efs=[] } in
    let rec loop = function
      | [] -> [item]
      | h::t when mw_equals h item -> h::t
      | h::t when id < h.first -> item :: t
      | h::t -> h::(loop t) in
    loop multiwords

  let normalize_multiwords t =
    let new_multiwords = List.fold_left
      (fun acc line ->
        match (get_feat "_UD_mw_fusion" line, get_feat "_UD_mw_span" line) with
        | (None, None) -> acc
        | (Some fusion, Some string_span) ->
          let span =
            try int_of_string string_span
            with Failure _ -> error "[Conll, %s%s], _UD_mw_span must be integer" (sof t.file) (get_line_num t) in
          insert_multiword (fst line.id) span fusion acc
        | _ -> error "[Conll, %s%s], inconsistent mw specification" (sof t.file) (get_line_num t)
      )
      t.multiwords t.lines in
    { t with
      multiwords = new_multiwords;
      lines = List.map
        (fun l ->
          l
          |> remove_feat "_UD_mw_fusion"
          |> remove_feat "_UD_mw_span"
        ) t.lines
    }
  (* ---------- adding multiwords lines from features ---------- *)



  (* ========== retrieving or building full text on a sentence ========== *)
  let concat_words words = Sentence.fr_clean_spaces (String.concat "" words)

  let final_space line =
    try if List.assoc "SpaceAfter" line.efs = "No" then "" else " "
    with Not_found -> " "

  let build_sentence t =
    let rec loop = function
    | ([],[]) -> []
    | ([line],[]) -> [line.form]
    | (line::tail,[]) -> (line.form ^ (final_space line)) :: (loop (tail,[]))
    | (line::tail, ((mw::_) as multiwords)) when (fst line.id) < mw.first -> (line.form ^ (final_space line)) :: (loop (tail,multiwords))
    | (line::tail, ((mw::_) as multiwords)) when (fst line.id) = mw.first -> mw.fusion :: (loop (tail,multiwords))
    | (line::tail, (mw::mw_tail)) when (fst line.id) > mw.last -> (loop (line::tail,mw_tail))
    | (_::tail, multiwords) -> (loop (tail,multiwords))
    | (_, mw::_) ->
      (match mw.mw_line_num with
       | Some l -> error "[Conll, %sline %d] Inconsistent multiwords" (sof t.file) l
       | None -> error "[Conll, %s] Inconsistent multiwords" (sof t.file)
      ) in
    let form_list = loop (t.lines, t.multiwords) in
    concat_words form_list

  let get_sentence {meta; lines} =
    let rec loop = function
    | [] -> None
    | line::tail ->
      match Str.full_split (Str.regexp "# ?\\(sentence-\\)?text ?[:=]?[ \t]?") line with
      | [Str.Delim _; Str.Text t] -> Some t
      | _ -> loop tail in
    loop meta
  (* ---------- retrieving or building full text on a sentence ---------- *)

  let web_anno t =
    let buff = Buffer.create 32 in
    List.iter
      (fun line ->
        match line.deps with
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
        | _ -> error "[Conll.web_anno, line %d] multiple deps not handled" line.line_num
      ) t.lines;
    Buffer.contents buff

end (* module Conll *)



(* ======================================================================================================================== *)
module Conll_corpus = struct
  type t = (string * Conll.t) array

  let cpt = ref 0
  let res = ref []

  let reset () = cpt := 0; res := []

  (* add the current file to !res *)
  let load_one file =
    let lines = File.read file in

    let rev_locals = ref [] in
    let save_one () =
      (try
        let conll = Conll.parse_rev ~file !rev_locals in
        incr cpt;
        let base = Filename.basename file in
        let sentid = match Conll.get_sentid conll with Some id -> id | None -> sprintf "%s_%05d" base !cpt in
        res := (sentid,conll) :: !res
      with Conll.Empty_conll -> ());
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

  let load_list file_list =
    reset ();
    List.iter load_one file_list;
    Array.of_list (List.rev !res)

  let load file = load_list [file]

  let save file_name t =
    let out_ch = open_out file_name in
    Array.iter (fun (_,conll) -> fprintf out_ch "%s\n" (Conll.to_string conll)) t;
    close_out out_ch

  let dump t =
    Array.iter (fun (_,conll) -> printf "%s\n" (Conll.to_string conll)) t

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

end

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
    |> (String_map.find dep))
    with Not_found -> None

  let table buff stat label =
    bprintf buff "    <table>\n";
    bprintf buff "      <thead>\n";
    bprintf buff "        <tr>\n";
    bprintf buff "          <th/>\n";
    String_set.iter (fun tag -> bprintf buff "            <th>%s</th>\n" tag) stat.tags;
    bprintf buff "        </tr>\n";
    bprintf buff "      </thead>\n";
    bprintf buff "      <tbody>\n";
    String_set.iter (fun gov ->
      bprintf buff "        <tr>\n";
      bprintf buff "          <th>%s</th>\n" gov;
        String_set.iter (fun dep ->

          bprintf buff "          <td>%s</td>\n"
          (match get stat gov label dep with
            | Some i -> sprintf
            "                <button class=\"btn btn-primary\" onclick='javascript:%s(\"%s\",\"%s\",\"%s\")'>%d</button>\n"
            (match stat.key with Upos -> "u_explore" | Xpos -> "x_explore") gov label dep i
            | None -> "")
        ) stat.tags;
      bprintf buff "        </tr>\n";
    ) stat.tags;
    bprintf buff "      </tbody>\n";
    bprintf buff "    </table>\n";
    ()

  let escape_dot s = Str.global_replace (Str.regexp "\\.") "__" s

  let to_html stat =
    let buff = Buffer.create 32 in
    bprintf buff "<div class=\"navbar\" id=\"right-navbar\">\n";
    bprintf buff "  <div class=\"navbar-inner\">\n";
    bprintf buff "    <ul class=\"nav nav-pills\">\n";
    String_set.iter
      (fun label ->
        bprintf buff
        "      <li><a class=\"explore-label\" href=\"#%s\" data-toggle=\"tab\">%s</a></li>\n"
        (escape_dot label) label
      ) stat.labels;
    bprintf buff "    </ul>\n";
    bprintf buff "  </div>\n";
    bprintf buff "</div>\n";

    bprintf buff "<div class=\"tab-content\">\n";
    String_set.iter
      (fun label ->
        bprintf buff "  <div class=\"tab-pane\" id=\"%s\">\n" (escape_dot label);
        table buff stat label;
        bprintf buff "  </div>\n";
      ) stat.labels;
    bprintf buff "</div>\n";

    Buffer.contents buff



end
