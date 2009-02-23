%%%----------------------------------------------------------------------
%%% File    : sauron.erl
%%% Author  : Elias Torres <elias@torrez.us>
%%% Purpose : Erlang MongoDB BSON Manipulator
%%%----------------------------------------------------------------------

-module(mongodb_bson).
-author('elias@torrez.us').

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

bin_to_hexstr(Bin) ->
  lists:flatten([io_lib:format("~2.16.0B", [X]) ||
    X <- binary_to_list(Bin)]).

hexstr_to_bin(S) ->
  hexstr_to_bin(S, []).
hexstr_to_bin([], Acc) ->
  list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
  {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
  hexstr_to_bin(T, [V | Acc]).

encode({obj, Items}) when is_list(Items) ->
  Binary = encode_elements(Items, []),
  <<(size(Binary)+5):32/little-signed, Binary/binary, 0:8>>.

encode_elements([Item|Rest], Accum) ->
  encode_elements(Rest, [encode_element(Item) | Accum]);
  
encode_elements([], Accum) ->
  list_to_binary(lists:reverse(Accum)).

encode_element({Name, Value}) when is_list(Name), is_float(Value) ->
  NameEncoded = encode_cstring(Name),
  <<1, NameEncoded/binary,  Value:64/little-signed-float>>;
  
encode_element({Name, Value}) when is_list(Name), is_list(Value) ->
  NameEncoded = encode_cstring(Name),
  ValueEncoded = encode_cstring(Value),
  <<2, NameEncoded/binary, (size(ValueEncoded)):32/little-signed, ValueEncoded/binary>>;

encode_element({Name, {obj, Items}}) when is_list(Items) ->
  NameEncoded = encode_cstring(Name),
  Binary = encode({obj, Items}),
  <<3, NameEncoded/binary, Binary/binary>>;

encode_element({Name, {list, []}}) ->
  NameEncoded = encode_cstring(Name),
  <<4, NameEncoded/binary, 5:32/little-signed, 0:8>>;

encode_element({Name, {list, Items}}) when is_list(Items) ->
  NameEncoded = encode_cstring(Name),
  ItemNames = [integer_to_list(Index) || Index <- lists:seq(0, length(Items)-1)],
  ItemList = lists:zip(ItemNames, Items),
  Binary = encode({obj, ItemList}),
  <<4, NameEncoded/binary, Binary/binary>>;

encode_element({Name, {binary, 2, Data}}) when is_list(Name) ->
  StringEncoded = encode_cstring(Name),
  <<5, StringEncoded/binary, (size(Data)+4):32/little-signed, 2:8, (size(Data)):32/little-signed, Data/binary>>;

encode_element({Name, {binary, SubType, Data}}) when is_list(Name) ->
  StringEncoded = encode_cstring(Name),
  <<5, StringEncoded/binary, (size(Data)):32/little-signed, SubType:8, Data/binary>>;

encode_element({Name, {oid, <<First:8/little-binary-unit:8, Second:4/little-binary-unit:8>>}}) when is_list(Name) ->
  StringEncoded = encode_cstring(Name),
  FirstReversed = lists:reverse(binary_to_list(First)),
  SecondReversed = lists:reverse(binary_to_list(Second)),
  OID = list_to_binary(lists:append(FirstReversed, SecondReversed)),
  <<7, StringEncoded/binary, OID/binary>>;

encode_element({Name, true}) when is_list(Name) ->
  StringEncoded = encode_cstring(Name),
  <<8, StringEncoded/binary, 1:8>>;

encode_element({Name, false}) when is_list(Name) ->
  StringEncoded = encode_cstring(Name),
  <<8, StringEncoded/binary, 1:8>>;

encode_element({Name, {MegaSecs, Secs, MicroSecs}}) 
      when  is_list(Name), 
            is_integer(MegaSecs), 
            is_integer(Secs), 
            is_integer(MicroSecs) ->
  StringEncoded = encode_cstring(Name),
  Unix = MegaSecs * 1000000 + Secs,
  Millis = Unix * 1000 + trunc(MicroSecs / 1000),
  <<9, StringEncoded/binary, Millis:64/little-signed>>;
  
encode_element({Name, null}) when is_list(Name) ->
  StringEncoded = encode_cstring(Name),
  <<10, StringEncoded/binary>>;

encode_element({Name, {regex, Expression, Flags}}) when is_list(Name), is_list(Expression), is_list(Flags) ->
  StringEncoded = encode_cstring(Name),
  ExpressionEncoded = encode_cstring(Expression),
  FlagsEncoded = encode_cstring(Flags),
  <<11, StringEncoded/binary, ExpressionEncoded/binary, FlagsEncoded/binary>>;

encode_element({Name, {ref, Collection, 
      <<First:8/little-binary-unit:8, Second:4/little-binary-unit:8>>}}) 
          when is_list(Name), is_list(Collection) ->
  StringEncoded = encode_cstring(Name),
  CollectionEncoded = encode_cstring(Collection),
  FirstReversed = lists:reverse(binary_to_list(First)),
  SecondReversed = lists:reverse(binary_to_list(Second)),
  OID = list_to_binary(lists:append(FirstReversed, SecondReversed)),
  <<12, StringEncoded/binary, (size(CollectionEncoded)):32/little-signed, CollectionEncoded/binary, OID/binary>>;

encode_element({Name, {code, Code}}) when is_list(Name), is_list(Code) ->
  StringEncoded = encode_cstring(Name),
  CodeEncoded = encode_cstring(Code),
  <<13, StringEncoded/binary, (size(CodeEncoded)):32/little-signed, CodeEncoded/binary>>;

encode_element({Name, Value}) when is_list(Name), is_integer(Value) ->
  StringEncoded = encode_cstring(Name),
  <<16, StringEncoded/binary, Value:4/little-signed-unit:8>>.

encode_cstring(String) when is_list(String) ->
  UTF8 = xmerl_ucs:to_utf8(String),
  Size = length(UTF8),
  BinString = list_to_binary(UTF8),
  <<BinString:Size/little-binary-unit:8, 0:8>>.

%% Size has to be greater than 4
decode(<<Size:32/little-signed, Rest/binary>> = Binary) when byte_size(Binary) >= Size, Size > 4 ->
  decode(Rest, Size-4);

decode(_BadLength) ->
  throw({invalid_length}).

decode(Binary, _Size) ->
  case decode_next(Binary, []) of
    {BSON, <<>>} ->
      BSON;
    {BSON, Rest} when byte_size(Rest) > 0 ->
      [BSON | decode(Rest)]
  end.
  
decode_next(<<0:8, Rest/binary>>, Accum) ->
  {{obj, lists:reverse(Accum)}, Rest};

decode_next(<<Type:8/little, Rest/binary>>, Accum) ->
  {Name, EncodedValue} = decode_cstring(Rest, []),
  {Value, Next} = decode_value(Type, EncodedValue),
  decode_next(Next, [{Name, Value}|Accum]).
  
decode_cstring(<<>> = _Binary, _Accum) ->
  throw({invalid_cstring});
  
decode_cstring(<<0:8, Rest/binary>>, Accum) ->
  {xmerl_ucs:from_utf8(lists:reverse(Accum)), Rest};
  
decode_cstring(<<Char:8/little, Rest/binary>> = _Binary, Accum) ->
  decode_cstring(Rest, [Char|Accum]).

decode_value(_Type = 1, <<Double:64/little-signed-float, Rest/binary>>) ->
  {Double, Rest};

decode_value(_Type = 2, <<Size:32/little-signed, Rest/binary>>)
    when size(Rest) =:= Size + 1 ->
  decode_cstring(Rest, []);

decode_value(_Type = 3, <<Size:32/little-signed, Rest/binary>> = Binary) 
    when size(Binary) >= Size ->
  decode_next(Rest, []);

decode_value(_Type = 4, <<Size:32/little-signed, Data/binary>> = Binary) 
    when size(Binary) >= Size ->
  {{obj, Array}, Rest} = decode_next(Data, []),
  {{list,[Value || {_Key, Value} <- Array]}, Rest};

decode_value(_Type = 5, <<_Size:32/little-signed, 2:8/little, BinSize:32/little-signed, BinData:BinSize/binary-little-unit:8, Rest/binary>>) ->
  {{binary, 2, BinData}, Rest};
  
decode_value(_Type = 5, <<Size:32/little-signed, SubType:8/little, BinData:Size/binary-little-unit:8, Rest/binary>>) ->
  {{binary, SubType, BinData}, Rest};

decode_value(_Type = 6, _Binary) ->
  throw(encountered_undefined);

decode_value(_Type = 7, <<First:8/little-binary-unit:8, Second:4/little-binary-unit:8, Rest/binary>>) ->
  FirstReversed = lists:reverse(binary_to_list(First)),
  SecondReversed = lists:reverse(binary_to_list(Second)),
  OID = list_to_binary(lists:append(FirstReversed, SecondReversed)),
  {{oid, OID}, Rest};

decode_value(_Type = 8, <<0:8, Rest/binary>>) ->
  {false, Rest};

decode_value(_Type = 8, <<1:8, Rest/binary>>) ->
  {true, Rest};

decode_value(_Type = 9, <<Millis:64/little-signed, Rest/binary>>) ->
  UnixTime = trunc(Millis / 1000),
  MegaSecs = trunc(UnixTime / 1000000),
  Secs = UnixTime - (MegaSecs * 1000000),
  MicroSecs = (Millis - (UnixTime * 1000)) * 1000,
  {{MegaSecs, Secs, MicroSecs}, Rest};

decode_value(_Type = 10, Binary) ->
  {null, Binary};

decode_value(_Type = 11, Binary) ->
  {Expression, RestWithFlags} = decode_cstring(Binary, []),
  {Flags, Rest} = decode_cstring(RestWithFlags, []),
  {{regex, Expression, Flags}, Rest};

decode_value(_Type = 12, <<Size:32/little-signed, Data/binary>> = Binary) when size(Binary) >= Size ->
  {NS, RestWithOID} = decode_cstring(Data, []),
  {{oid, OID}, Rest} = decode_value(7, RestWithOID),
  {{ref, NS, OID}, Rest};

decode_value(_Type = 13, <<_Size:32/little-signed, Data/binary>>) ->
  {Code, Rest} = decode_cstring(Data, []),
  {{code, Code}, Rest};

decode_value(_Type = 14, _Binary) ->
  throw(encountered_ommitted);

decode_value(_Type = 15, _Binary) ->
  throw(encountered_ommitted);

decode_value(_Type = 16, <<Integer:32/little-signed, Rest/binary>>) ->
  {Integer, Rest}.

bson_test_() ->

  Tests = [ 
      {"0c0000001061000100000000", {obj,[{"a", 1}]}},
      {"13000000106100010000001062000200000000", {obj,[{"a", 1},{"b", 2}]}},
      {"1300000002610007000000737472696e670000", {obj, [{"a", "string"}]}},
      {"10000000016100333333333333f33f00", {obj, [{"a",1.2}]}},
      {"140000000361000c000000106200010000000000", {obj, [{"a", {obj, [{"b", 1}]}}]}},
      {"090000000861000100", {obj, [{"a", true}]}},
      {"080000000a610000", {obj, [{"a", null}]}},
      {"10000000096100355779901f01000000", {obj, [{"a", {1235,79485,237000}}]}}, % datetime.datetime(2009, 2, 19, 21, 38, 5, 237009)
      {"16000000076f69640007060504030201000b0a090800", {obj, [{"oid", {oid, <<0,1,2,3,4,5,6,7,8,9,10,11>>}}]}},
      {"120000000b726567657800612a6200690000", {obj, [{"regex", {regex, "a*b", "i"}}]}},
      {"1f0000000c7265660005000000636f6c6c0007060504030201000b0a090800", {obj, [{"ref", {ref, "coll", <<0,1,2,3,4,5,6,7,8,9,10,11>>}}]}},
      {"160000000D2477686572650005000000746573740000", {obj, [{"$where", {code, "test"}}]}},
      {"1100000004656D70747900050000000000", {obj, [{"empty", {list,[]}}]}},
      {"26000000046172726179001a0000001030000100000010310002000000103200030000000000", {obj, [{"array", {list,[1,2,3]}}]}},
      {"180000000574657374000800000002040000007465737400", {obj,[{"test", {binary, 2, <<"test">>}}]}},
      {"1400000005746573740004000000807465737400", {obj,[{"test", {binary, 128, <<"test">>}}]}},
      {"1e0000000274657374000f000000c3a9e0afb2e0be84e19db0e38eaf0000", {obj, [{"test", [233,3058,3972,6000,13231]}]}}
  ],

  ToErlang = [?_assertEqual(Expected, decode(hexstr_to_bin(Hex))) || {Hex, Expected} <- Tests],
  
  FromErlang = [?_assertEqual(hexstr_to_bin(Hex), encode(Expected)) || {Hex, Expected} <- Tests],
  
  lists:append([ToErlang, FromErlang]).
  
bson_exception_test_() ->
  
  ExceptionTests = [
    {"0400000000", {invalid_length}},
    {"0500000001", {invalid_cstring}},
    {"05000000", {invalid_length}},
    {"050000000000", {invalid_length}},
    {"0c00", {invalid_length}}
  ],
  
  [?_assertThrow(Expected, decode(hexstr_to_bin(Hex))) || {Hex, Expected} <- ExceptionTests].