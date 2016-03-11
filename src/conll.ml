open Printf
open Log

(* ======================================================================================================================== *)
module List_ = struct
  let to_string string_of_item sep = function
    | [] -> ""
    | h::t -> List.fold_left (fun acc elt -> acc ^ sep ^ (string_of_item elt)) (string_of_item h) t
  
  let mapi fct =
    let rec loop i = function
      | [] -> []
      | h::t -> (fct i h) :: (loop (i+1) t)
    in loop 0
end

(* ======================================================================================================================== *)
module File = struct
  let read_rev file =
    let in_ch = open_in file in
    (* if the input file contains an UTF-8 byte order mark (EF BB BF), skip 3 bytes, else get back to 0 *)
    (match input_byte in_ch with 0xEF -> seek_in in_ch 3 | _ -> seek_in in_ch 0);

    let line_num = ref 0 in
    let res = ref [] in
    try
      while true do
        incr line_num;
        res := (!line_num, input_line in_ch) :: !res
      done; assert false
    with End_of_file -> close_in in_ch; !res

  let read file = List.rev (read_rev file)

end

(* ======================================================================================================================== *)
module Conll = struct
  
  (* ------------------------------------------------------------------------ *)
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

  let root = { line_num = -1; id=0; form="ROOT"; lemma="__"; upos="_X"; xpos=""; feats=[]; deps=[] }

  let compare l1 l2 = Pervasives.compare l1.id l2.id

  let line_to_string l =
    let (gov_list, lab_list) = List.split l.deps in
    sprintf "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t_\t_"
      l.id l.form l.lemma l.upos l.xpos
      (match l.feats with [] -> "_" | list -> String.concat "|" (List.map (fun (f,v) -> sprintf "%s=%s" f v) list))
      (List_.to_string string_of_int "|" gov_list)
      (String.concat "|" lab_list)

  (* ------------------------------------------------------------------------ *)
  type multiword = {
    first: int;
    last: int;
    fusion: string;
  }

  let multiword_to_string l =
    sprintf "%d-%d\t%s\t_\t_\t_\t_\t_\t_\t_\t_" l.first l.last l.fusion

  (* ------------------------------------------------------------------------ *)
  type t = {
    file: string option;
    meta: string list;
    lines: line list;
    multiwords: multiword list;
  }

  let sof = function 
    | Some f -> sprintf "File %s, " f 
    | None -> ""

  let empty file = { file;  meta=[]; lines=[]; multiwords=[] }

  let parse_feats ~file line_num = function
    | "_" -> []
    | feats ->
      List.map
        (fun feat ->
          match Str.split (Str.regexp "=") feat with
            | [feat_name] -> (feat_name, "true")
            | [feat_name; feat_value] -> (feat_name, feat_value)
            | _ -> Log.fcritical "[Conll, %sline %d], cannot parse feats \"%s\"" (sof file) line_num feats
        ) (Str.split (Str.regexp "|") feats)

  let underscore s = if s = "" then "_" else s

  (* parse a list of line corresponding to one conll structure *)
  let parse_rev ?file lines =
    List.fold_left
      (fun acc (line_num, line) ->
        if line.[0] = '#'
        then {acc with meta = line :: acc.meta}
        else
          match Str.split (Str.regexp "\t") line with
            | [ f1; form; lemma; upos; xpos; feats; govs; dep_labs; _; _ ] ->
            begin
              try
                match Str.split (Str.regexp "-") f1 with
                | [f;l] -> {acc with multiwords = {first=int_of_string f; last=int_of_string l; fusion=form}:: acc.multiwords}
                | [string_id] ->
                  let gov_list = if govs = "_" then [] else List.map int_of_string (Str.split (Str.regexp "|") govs)
                  and lab_list = if dep_labs = "_" then [] else Str.split (Str.regexp "|") dep_labs in
                  let deps = List.combine gov_list lab_list in
                  let new_line =
                    {
                    line_num = line_num;
                    id = int_of_string string_id;
                    form = underscore form;
                    lemma = underscore lemma;
                    upos = underscore upos;
                    xpos = underscore xpos;
                    feats = parse_feats ~file line_num feats;
                    deps = deps;
                    } in
                  {acc with lines = new_line :: acc.lines }
                | _ -> Log.fcritical "[Conll, %sline %d], illegal field one \"%s\"" (sof file) line_num f1
              with exc -> Log.fcritical "[Conll, %sline %d], unexpected exception \"%s\" in line \"%s\"" (sof file) line_num (Printexc.to_string exc) line
            end
            | l -> Log.fcritical "[Conll, %sline %d], illegal line, %d fields (10 are expected)\n>>>>%s<<<<" (sof file) line_num (List.length l) line
      ) (empty file) lines

  let parse ?file lines = parse_rev ?file (List.rev lines)

  let from_string s =
    let lines = Str.split (Str.regexp "\n") s in
    let num_lines = List_.mapi (fun i l -> (i+1,l)) lines in
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
      | (line::tail, {first}::_) when line.id < first ->
          bprintf buff "%s\n" (line_to_string line); loop (tail,multiwords)
      | (_, mw::tail) ->
          bprintf buff "%s\n" (multiword_to_string mw); loop (lines,tail) in
    loop (t.lines, t.multiwords);
    Buffer.contents buff

  let get_sentid {meta; lines} =
    let fs1 = (List.hd lines).feats in
    try Some (List.assoc "sentid" fs1)
    with Not_found ->
    try Some (List.assoc "sent_id" fs1)
    with Not_found ->
    let rec loop = function
    | [] -> None
    | line::tail ->
      match Str.full_split (Str.regexp "# sent_?id:?[ \t]?") line with
      | [Str.Delim _; Str.Text t] -> Some t
      | _ -> loop tail in
    loop meta
end

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
      incr cpt;
      let conll = Conll.parse_rev ~file !rev_locals in
      let sentid = match Conll.get_sentid conll with Some id -> id | None -> sprintf "%s_%05d" file !cpt in 
      res := (sentid,conll) :: !res;
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
end
