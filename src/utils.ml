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
