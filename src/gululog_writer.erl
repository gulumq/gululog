%% @doc The log (topic) writer (owner) API module.

-module(gululog_writer).

-behaviour(gen_server).

%% APIs
-export([ start/1
        , start_link/1
        , stop/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-type index() :: gululog_idx:index().
-type w_cur() :: gululog_w_cur:cursor().

-define(DEFAULT_SEG_MB, 100). %% default segment size in MB

-define(RECORD_TO_PROPLIST(RECORD_NAME, THE_TUPLE),
        {RECORD_NAME,
         lists:zip(record_info(fields, RECORD_NAME),
                   tl(tuple_to_list(THE_TUPLE)))}).

-define(FIELD_ERR(RECORD_NAME, FIELD_NAME),
        erlang:error({field_value_missing, RECORD_NAME, FIELD_NAME})).

-define(ST_FIELD_ERR(FIELD_NAME), ?FIELD_ERR(state, FIELD_NAME)).

-record(state,
        { dir   = ?ST_FIELD_ERR(dir)   :: dirname()        %% index / segment file location
        , segMB = ?ST_FIELD_ERR(segMB) :: bytecnt()        %% segment file size limit in MB
        , index                        :: ?undef | index() %% index
        , w_cur                        :: ?undef | w_cur() %% writer causor
        }).

%%%*_ API FUNCTIONS ============================================================

start(Options) ->
  gen_server:start(?MODULE, Options, []).

start_link(Options) ->
  gen_server:start_link(?MODULE, Options, []).

stop(Pid) ->
  gen_server:cast(Pid, stop).

%%%*_ gen_server CALLBACKS =====================================================

init(Options) ->
  Dir = keyget(key, Options),
  SegMB = keyget(segMB, Options, ?DEFAULT_SEG_MB),
  gen_server:cast(self(), post_init),
  {ok, #state{dir = Dir, segMB = SegMB}}.

handle_cast(post_init, #state{} = State) ->
  NewState = do_init(State),
  {noreply, NewState};
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

handle_call(_Call, _From, State) ->
  {reply, {error, no_impl}, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{index = Index, w_cur = W_cur} = _State) ->
  ok = maybe_close_index(Index),
  ok = maybe_close_w_cur(W_cur),
  %% NewState = State#state{index = ?undef, w_cur = unfefined},
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

format_status(_Opt, [_PDict, State]) ->
  Data = ?RECORD_TO_PROPLIST(state, State),
  [{data, [{"StateData", Data}]}].

%%%*_ PRIVATE FUNCTIONS ========================================================

%% TODO: 1. cut off corrupted index tail
%%       2. open index
%%       3. cut off corrupted segment tail
%%       4. open writer cursor
do_init(State) -> State.

%% @private Get value from key-value list, crash if not found.
-spec keyget(Key::term(), [{Key::term(), Value::term()}]) -> Value::term().
keyget(Key, KvList) ->
  {Key, Value} = lists:keyfind(Key, 1, KvList),
  Value.

%% @private Get value from key-value list, return default if not found
-spec keyget(Key::term(), [{Key::term(), Value::term()}], Default::term()) -> Value::term().
keyget(Key, KvList, Default) ->
  case lists:keyfind(Key, 1, KvList) of
    {Key, Value} -> Value;
    false        -> Default
  end.

-spec maybe_close_index(?undef | index()) -> ok.
maybe_close_index(?undef) -> ok;
maybe_close_index(Index)  -> gululog_idx:flush_close(Index).

-spec maybe_close_w_cur(?undef | w_cur()) -> ok.
maybe_close_w_cur(?undef) -> ok;
maybe_close_w_cur(W_cur)  -> gululog_w_cur:flush_close(W_cur).

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

dir_missing_test() ->
  ?assertException(error, {field_value_missing, state, dir}, #state{}),
  ?assertException(error, {field_value_missing, state, segMB}, #state{dir = ""}).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
