% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric2_db_expiration).


-behaviour(gen_server).


-export([
    start_link/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/include/fabric2.hrl").


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    case wait_couch_job() of
        ok -> ok;
        retry -> wait_couch_job()
    end,
    case is_enabled() of
        true ->
            process_expiration();
        false ->
            ok
    end,
    {ok, nil}.


terminate(_M, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


wait_couch_job() ->
    try
        couch_jobs:set_type_timeout(?DB_EXPIRATION_JOB_TYPE, 6),
        couch_jobs:add(undefined, ?DB_EXPIRATION_JOB_TYPE, ?DB_EXPIRATION_JOB, #{}),
        {ok, _Job, _JobData} = couch_jobs:accept(?DB_EXPIRATION_JOB_TYPE,
            #{max_sched_time => 1000}),
        ok
    catch
        error:badarg ->
            retry
    end.


process_expiration() ->
    Callback = fun(Value, Acc) ->
        NewAcc = case Value of
            {meta, _} -> Acc;
            {row, DbInfo} ->
                process_row(Acc, DbInfo);
            complete ->
                TotalLen = length(Acc),
                if TotalLen == 0 -> Acc; true ->
                    [{LastDelete, _, _} | _] = Acc,
                    TotalLen = length(Acc),
                    delete_dbs(lists:sublist(Acc, TotalLen - LastDelete)),
                    Acc
                end
            end,
        {ok, NewAcc}
    end,
    {ok, _Infos} = fabric2_db:list_deleted_dbs_info(Callback, [], []).


process_row(Acc, DbInfo) ->
    TotalLen = length(Acc),
    case TotalLen of
        0 ->
            DbName = proplists:get_value(db_name, DbInfo),
            TimeStamp = proplists:get_value(timestamp, DbInfo),
            [{0, DbName, TimeStamp}];
        _ ->
            [{LastDelete, _, _} | _] = Acc,
            NumberToDelete = TotalLen - LastDelete,
            DeleteBatch = expiration_batch(),
            LastDelete2 = case NumberToDelete == DeleteBatch of
                true ->
                    delete_dbs(lists:sublist(Acc, DeleteBatch)),
                    TotalLen;
                _ ->
                    LastDelete
            end,
            DbName = proplists:get_value(db_name, DbInfo),
            TimeStamp = proplists:get_value(timestamp, DbInfo),
            [{LastDelete2, DbName, TimeStamp} | Acc]
    end.


delete_dbs(Infos) ->
    lists:foreach(fun({_, DbName, TimeStamp}) ->
        Now = now_sec(),
        Retention = retention_sec(),
        Since = Now - Retention,
        case Since > timestamp_to_sec(TimeStamp)  of
            true ->
                ok = fabric2_db:delete(DbName, [{deleted_at, TimeStamp}]);
            false ->
                ok
        end
    end, Infos).


now_sec() ->
    Now = os:timestamp(),
    Nowish = calendar:now_to_universal_time(Now),
    calendar:datetime_to_gregorian_seconds(Nowish).


timestamp_to_sec(TimeStamp) ->
    <<Year:4/binary, "-", Month:2/binary, "-", Day:2/binary,
        "T",
        Hour:2/binary, ":", Minutes:2/binary, ":", Second:2/binary,
        "Z">> = TimeStamp,

    calendar:datetime_to_gregorian_seconds(
        {{?bin2int(Year), ?bin2int(Month), ?bin2int(Day)},
            {?bin2int(Hour), ?bin2int(Minutes), ?bin2int(Second)}}
    ).


is_enabled() ->
    config:get_boolean("couch", "db_expiration_enabled", true).


retention_sec() ->
    config:get_integer("couch", "db_expiration_retention_sec",
        ?DEFAULT_RETENTION_SEC).


expiration_batch() ->
    config:get_integer("couch", "db_expiration_batch",
        ?DEFAULT_EXPIRATION_BATCH).
