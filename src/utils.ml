open Printf

(* ======================================================================================================================== *)
module List_ = struct
  let to_string string_of_item sep = function
    | [] -> ""
    | h::t -> List.fold_left (fun acc elt -> acc ^ sep ^ (string_of_item elt)) (string_of_item h) t

  let rec opt_map f = function
    | [] -> []
    | x::t -> match f x with
      | None -> opt_map f t
      | Some r -> r :: (opt_map f t)
end

(* ======================================================================================================================== *)
module File = struct
  let read_rev file =
    let in_ch = open_in file in
    let line_num = ref 0 in
    let res = ref [] in
    try

      (* if the input file contains an UTF-8 byte order mark (EF BB BF), skip 3 bytes, else get back to 0 *)
      (match input_byte in_ch with 0xEF -> seek_in in_ch 3 | _ -> seek_in in_ch 0);

      while true do
        incr line_num;
        res := (!line_num, input_line in_ch) :: !res
      done; assert false
    with End_of_file -> close_in in_ch; !res

  let read file = List.rev (read_rev file)
end
