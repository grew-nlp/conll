open Printf
open Conll

module Array_ = struct
  exception Found of int
  let index fct a =
    try Array.iteri (fun i elt -> if (fct elt) then raise (Found i)) a; None
    with Found i -> Some (i,a.(i))
end

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

let sentid corpus =
	let new_corpus = Array.map
		(fun (id, conll) -> (id, Conll.ensure_sentid_in_meta conll)
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

let print_usage () =
	List.iter (fun x -> printf "%s\n" x)
	[
	"Usage: conll_tool.native <subcommand> <args>";
	"subcommand are:"
	" * sentences <file>       dump on stdout the list of \"id#sentence\"";
	" * sentid <file>          dump the input corpus with #sentid moved into metadata when necessary";
	" * random <file> <num>    split input <file> corpus into a randomly selected subset on sentence large enough to have at least <num> tokens";
	"                          output is stored in two files with extension _sub.conll (the extracted part) and _rem.conll (the remaining sentences)";
	]

let _ =
	match List.tl (Array.to_list Sys.argv) with

	(* pat stands for post annot_tool *)
	| ["pat"; corpus_name; at_file] ->
		let corpus = Conll_corpus.load corpus_name in
		let at_list = List.map snd (File.read at_file) in
		List.iter (fun at ->
			match Str.split (Str.regexp "__\\|#\\|\\.svg#") at with
			| [sentid; pos; lab] ->
				begin
					match Array_.index (fun (id,_) -> id=sentid) corpus with
					| None -> printf "ERROR: sentid \"%s\" not found in corpus\n" sentid; exit 1
					| Some (i,(_,conll)) ->
						let new_conll = Conll.set_label (int_of_string pos) lab conll in
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
			(Array.of_list (List.sort (fun (id1,_) (id2,_) -> Pervasives.compare id1 id2) sub));
		Conll_corpus.save (basename^"_rem.conll") (Array.of_list rem)
	| "random"::_ -> printf "ERROR: sub-command \"random\" expects two arguments\n"; print_usage ()

	| ["split"; corpus_name; id_file] ->
		let basename = Filename.basename corpus_name in
		let corpus = Conll_corpus.load corpus_name in
		let id_list = List.map snd (File.read id_file) in
		let (c_in, c_out) = split corpus id_list in
		Conll_corpus.save (basename^"_in.conll") c_in;
		Conll_corpus.save (basename^"_out.conll") c_out;
		printf "Splitting; IN: %d sentences, %d tokens; OUT: %d sentences, %d tokens\n"
			(Array.length c_in)
			(Conll_corpus.token_size c_in)
			(Array.length c_out)
			(Conll_corpus.token_size c_out)
	| "split"::_ -> printf "ERROR: sub-command \"split\" expects two arguments\n"; print_usage ()

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
	| "dot"::_ -> printf "ERROR: sub-command \"dot\" expects one argument\n"; print_usage ()

	| [] -> print_usage ()
	| x :: _ ->
		printf "ERROR: unknown sub-command \"%s\"\n" x; print_usage ()