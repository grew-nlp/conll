open Printf
open Conll
open Conllx

let rec list_extract n = function
  | [] -> failwith "list_extract"
  | h::t when n=0 -> (h,t)
  | h::t -> let (x,r) = list_extract (n-1) t in (x,h::r)

let dump_id_sentence corpus =
  Array.iter
    (fun (id,conll) ->
       let sentence = match Conll.get_sentence conll with
         | Some s -> s
         | None -> Conll.build_sentence conll in
       printf "%s#%s\n" id sentence
    ) corpus

let add_text corpus =
  Array.map
    (fun (id,conll) ->
       match Conll.get_sentence conll with
       | Some _ -> (id,conll)
       | None -> (id,{conll with meta = conll.Conll.meta @ [(sprintf "# text = %s" (Conll.build_sentence conll))] })
    ) corpus

let sentid corpus =
  let new_corpus = Array.map
      (fun (id, conll) ->
         (id, Conll.normalize_multiwords (Conll.ensure_sentid_in_meta ~default:id conll))
      ) corpus in
  Conll_corpus.dump new_corpus

let fusion corpus =
  let new_corpus = Array.map
      (fun (id, conll) -> (id, Conll.normalize_multiwords conll)
      ) corpus in
  Conll_corpus.dump new_corpus

let split corpus id_list =
  let (i,o) = List.partition
      (function
        | (id,conll) when List.mem id id_list -> true
        | _ -> false
      ) (Array.to_list corpus) in
  (Array.of_list i, Array.of_list o)

let sub corpus id_list =
  let sub_list = CCList.filter_map
      (fun id ->
         CCArray.find_map (fun (i,c) -> if i=id then Some (i,c) else None) corpus
      ) id_list in
  Array.of_list sub_list

let print_usage () =
  List.iter (fun x -> printf "%s\n" x)
    [
      "Usage: conll_tool.native <subcommand> <args>";
      "subcommands are:";
      " * normalize <input_corpus_file> <output_corpus_file>";
      "      dump the input corpus in a normalized way (sorting of features, …)";
      " * sentences <corpus_file>";
      "      dump on stdout the list of \"id#sentence\" contained in the <corpus_file>";
      " * sentid <corpus_file>";
      "      dump the input <corpus_file> with #sentid moved from features into metadata when necessary";
      " * add_text <input_corpus_file> <output_corpus_file>";
      "      add the 'text' meta data build from conll line (for French)";
      " * random <corpus_file> <num>";
      "      split input <corpus_file> into a randomly selected subset on sentence large enough to have at least <num> tokens";
      "      output is stored in two files with extension _sub.conll (the extracted part) and _rem.conll (the remaining sentences)";
      " * split <corpus_file> <id_file>";
      "      split input <corpus_file> in two files with extension _in.conll (the sentid belongs to <id_file>) and _out.conll (the remaining sentences)";
      " * order <corpus_file> <id_file>";
      "      order input <corpus_file> follwing id giveni in <id_file>";
      " * fusion <corpus_file>";
      "      dump the input <corpus_file> with new lines for fusion words (data taken from _UD_mw_span and _UD_mw_fusion special features)";
      " * web_anno <corpus_file> <basename> <size>";
      "      split <corpus_file> into several set of <size> sentences for inclusion in web_anno. Output files are nammed <basename>_xx.conll";
      " * cut <corpus_file> <basename> <size>";
      "      split <corpus_file> into several set of <size> sentences (metadata are kept, unlike in web_anno subcommand). Output files are nammed <basename>_xx.conll";
      " * ustat <corpus_id> <corpus_file>";
      "      output the list of triple (xpos_gov, label, xpos_dep) with the number of occurences. Output is a list of lines like this one:";
      "      PRON -[obl]-> NOUN ==> 10";
      "      also build a local file <corpus_id>_utable.php with the html code for stat browsing";
      " * xstat <corpus_id> <corpus_file>";
      "      same as upos with XPOS stat insted of UPOS";
      " * pat <corpus_file> <patch_file>";
      "      apply a patch file produced by annot_tool (pat stands for \"Post Annot Tool\")";
      " * merge <sentid1> <sentid2> <new_sentid> <input_corpus_file> <output_corpus_file>";
      "      merge two sentences in one";

    ]

let _ =
  match List.tl (Array.to_list Sys.argv) with

  (* pat stands for post annot_tool *)
  | ["pat"; corpus_name; at_file] ->
    let corpus = Conll_corpus.load corpus_name in
    let at_list = CCIO.(with_in at_file read_lines_l) in
    List.iter (fun at ->
        match Str.split (Str.regexp "__\\|#\\|\\.svg#") at with
        | [sentid; pos; lab] ->
          begin
            match CCArray.find_idx (fun (id,_) -> id=sentid) corpus with
            | None -> printf "ERROR: sentid \"%s\" not found in corpus\n" sentid; exit 1
            | Some (i,(_,conll)) ->
              let new_conll = Conll.set_label (Conll.Id.of_string pos) lab conll in
              corpus.(i) <- (sentid,new_conll)
          end
        | _ -> printf "ERROR: cannot parse annot_tool output \"%s\"\n" at; exit 1
      ) at_list;
    Conll_corpus.dump corpus
  | "pat"::_ -> printf "ERROR: sub-command \"pat\" expects two arguments\n"; print_usage ()

  | ["random"; corpus_name; min_tokens_string] ->
    let basename = Filename.basename corpus_name in
    let corpus = Conll_corpus.load corpus_name in
    Random.self_init ();
    let min_tokens =
      try int_of_string min_tokens_string
      with Failure _ -> printf "ERROR: sub-command \"random\" second arg must be int\n"; print_usage (); exit 0 in
    let full_size = Array.length corpus in
    let full_list = Array.to_list corpus in
    let rec loop size bound (sub, rem) =
      if bound < 0
      then (sub, rem)
      else
        let n = Random.int size in
        let ((id,conll),new_rem) = list_extract n rem in
        loop (size-1) (bound - (List.length conll.Conll.lines)) ((id,conll)::sub, new_rem) in
    let (sub,rem) = loop full_size min_tokens ([],full_list) in
    Conll_corpus.save (basename^"_sub.conll")
      (Array.of_list (List.sort (fun (id1,_) (id2,_) -> Stdlib.compare id1 id2) sub));
    Conll_corpus.save (basename^"_rem.conll") (Array.of_list rem)
  | "random"::_ -> printf "ERROR: sub-command \"random\" expects two arguments\n"; print_usage ()

  | ["split"; corpus_name; id_file] ->
    let basename = Filename.basename corpus_name in
    let corpus = Conll_corpus.load corpus_name in
    let id_list = CCIO.(with_in id_file read_lines_l) in
    let (c_in, c_out) = split corpus id_list in
    Conll_corpus.save (basename^"_in.conll") c_in;
    Conll_corpus.save (basename^"_out.conll") c_out;
    printf "Splitting; IN: %d sentences, %d tokens; OUT: %d sentences, %d tokens\n"
      (Array.length c_in)
      (Conll_corpus.token_size c_in)
      (Array.length c_out)
      (Conll_corpus.token_size c_out)
  | ["split"; corpus_name; id_file; corpus_in] ->
    let corpus = Conll_corpus.load corpus_name in
    let id_list = CCIO.(with_in id_file read_lines_l) in
    let c_in = sub corpus id_list in
    Conll_corpus.save (corpus_in) c_in
  | "split"::_ -> printf "ERROR: sub-command \"split\" expects two or three arguments\n"; print_usage ()


  | ["order"; corpus_name; id_file] ->
    let corpus = Conll_corpus.load corpus_name in
    let id_list = CCIO.(with_in id_file read_lines_l) in
    let new_corpus = Array.of_list (
        List.map
          (fun id -> match Conll_corpus.get id corpus with
            | Some g -> (id, g)
            | None -> failwith ("unkonwn id "^id)
          ) id_list
      ) in
    Conll_corpus.dump new_corpus


  | ["fusion"; corpus_name] ->
    let corpus = Conll_corpus.load corpus_name in
    fusion corpus
  | "fusion"::_ -> printf "ERROR: sub-command \"fusion\" expects one argument\n"; print_usage ()

  | ["sentences"; corpus_name] ->
    let corpus = Conll_corpus.load corpus_name in
    dump_id_sentence corpus
  | "sentences"::_ -> printf "ERROR: sub-command \"sentences\" expects one argument\n"; print_usage ()

  | ["sentid"; corpus_name] ->
    let corpus = Conll_corpus.load corpus_name in
    sentid corpus
  | "sentid"::_ -> printf "ERROR: sub-command \"sentid\" expects one argument\n"; print_usage ()

  | ["dot"; corpus_name] ->
    let corpus = Conll_corpus.load corpus_name in
    printf "%s" (Conll.to_dot (snd corpus.(0)))
  | ["dot"; corpus_name; output_file] ->
    let corpus = Conll_corpus.load corpus_name in
    Conll.save_dot output_file (snd corpus.(0))
  | "dot"::_ -> printf "ERROR: sub-command \"dot\" expects one argument\n"; print_usage ()

  | ["normalize"; corpus_in; corpus_out] ->
    let corpus = Conll_corpus.load corpus_in in
    Conll_corpus.save corpus_out corpus
  | "normalize"::_ -> printf "ERROR: sub-command \"normalize\" expects two arguments\n"; print_usage ()

  | ["add_text"; corpus_in; corpus_out] ->
    let corpus = Conll_corpus.load corpus_in in
    let new_corpus = add_text corpus in
    Conll_corpus.save corpus_out new_corpus
  | "add_text"::_ -> printf "ERROR: sub-command \"add_text\" expects two arguments\n"; print_usage ()

  | "ustat" :: corpus_id :: corpus_files ->
    let corpus = Conll_corpus.load_list corpus_files in
    let stat = Stat.build Stat.Upos corpus in
    Stat.dump stat;
    let html = Stat.to_html corpus_id stat in
    CCIO.with_out (corpus_id ^ "_utable.php") (fun oc -> CCIO.write_line oc html)
  | "ustat"::_ -> printf "ERROR: sub-command \"ustat\" expects at least two argument\n"; print_usage ()

  | "xstat" :: corpus_id :: corpus_files ->
    let corpus = Conll_corpus.load_list corpus_files in
    let stat = Stat.build Stat.Xpos corpus in
    Stat.dump stat;
    let html = Stat.to_html corpus_id stat in
    CCIO.with_out (corpus_id ^ "_xtable.php") (fun oc -> CCIO.write_line oc html)
  | "xstat"::_ -> printf "ERROR: sub-command \"xstat\" expects at least two argument\n"; print_usage ()

  | ["web_anno"; corpus_name; base_output; size_string] ->
    begin
      match int_of_string_opt size_string with
      | None -> printf "ERROR: sub-command \"web_anno\" third argument \"%s\" must be int \n" size_string; print_usage ()
      | Some size ->
        let corpus = Conll_corpus.load corpus_name in
        Conll_corpus.web_anno corpus base_output size
    end
  | "web_anno"::_ -> printf "ERROR: sub-command \"web_anno\" expects 3 arguments\n"; print_usage ()

  | ["merge"; sentid1; sentid2; new_sentid; corpus_in; corpus_out; ] ->
    let corpus = Conll_corpus.load corpus_in in
    (match (CCArray.find_idx (fun (id,_) -> id=sentid1) corpus, CCArray.find_idx (fun (id,_) -> id=sentid2) corpus) with
     | (Some (pos1,(id1,c1)), Some (pos2,(id2,c2))) ->
       if pos2 - pos1 <> 1
       then printf "ERROR Merge is possible only on consecutive sentences\n"
       else
         begin
           let new_conll = Conll.merge new_sentid c1 c2 in
           corpus.(pos1) <- (id1, new_conll);
           corpus.(pos2) <- ("__REMOVE__", Conll.void);
           Conll_corpus.save corpus_out corpus
         end
     | (None, _) -> printf "ERROR No index \"%s\"\n" sentid1
     | (_,None) -> printf "ERROR No index \"%s\"\n" sentid1
    )

  | "merge"::_ -> printf "ERROR: sub-command \"merge\" expects 5 arguments\n"; print_usage ()

  | ["cut"; corpus_in; base_output; subsize; ] ->
    begin
      match int_of_string_opt subsize with
      | None -> printf "ERROR: sub-command \"cut\" second argument \"%s\" must be int \n" subsize; print_usage ()
      | Some size ->
        let corpus = Conll_corpus.load corpus_in in
        let len = Array.length corpus in
        let last_ballot = (len-1) / size in
        for i = 0 to last_ballot do
          let out = sprintf "%s_%03d" base_output i in
          Conll_corpus.save_sub out (i*size) (min ((i+1)*size-1) (len-1)) corpus
        done
    end


  | "cut"::_ -> printf "ERROR: sub-command \"cut\" expects 3 arguments\n"; print_usage ()

  | ["sud_to_json"] ->
    begin
      try
        let cx = Conllx_corpus.read ~config:(Conllx_config.build "sud") () in
        Array.iter (fun (_,conllx) ->
            let json = Conllx.to_json conllx in
            Printf.printf "%s\n" (Yojson.Basic.pretty_to_string json)
          ) (Conllx_corpus.get_data cx)
      with
      | Conllx_error js -> printf " === Conllx_error === \n%s\n ====================\n" (Yojson.Basic.pretty_to_string js)
    end

  | ["seq_to_json"] ->
    begin
      try
        let cx = Conllx_corpus.read ~config:(Conllx_config.build "sequoia") () in
        Array.iter (fun (_,conllx) ->
            let json = Conllx.to_json conllx in
            Printf.printf "%s\n" (Yojson.Basic.pretty_to_string json)
          ) (Conllx_corpus.get_data cx)
      with
      | Conllx_error js -> printf " === Conllx_error === \n%s\n ====================\n" (Yojson.Basic.pretty_to_string js)
    end

  | ["sud_of_json"] ->
    begin
      try
        let json = Yojson.Basic.from_channel stdin in
        let conll = Conllx.of_json json in
        Printf.printf "%s\n" (Conllx.to_string ~config:(Conllx_config.build "sud") conll)
      with
      | Conllx_error js -> printf " === Conllx_error === \n%s\n ====================\n" (Yojson.Basic.pretty_to_string js)
    end

  | ["seq_of_json"] ->
    begin
      try
        let json = Yojson.Basic.from_channel stdin in
        let conll = Conllx.of_json json in
        Printf.printf "%s\n" (Conllx.to_string ~config:(Conllx_config.build "sequoia") ~columns:Conllx_columns.cupt conll)
      with
      | Conllx_error js -> printf " === Conllx_error === \n%s\n ====================\n" (Yojson.Basic.pretty_to_string js)
    end

  | [] -> print_usage ()
  | x :: _ -> printf "ERROR: unknown sub-command \"%s\"\n" x; print_usage ()
