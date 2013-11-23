{partial, is_array, is_function} = require 'libprotein'
{debug, warn, info, error} = (require 'console-logger').ns 'QueueManager'
{get_state, swap_state, watch_state} = require 'libstate'
{set_config} = require 'libconfig'

MY_STATE = 'queue-manager'

typeclass = (x) -> x.constructor

{CSQS, CSMTS, Type, TDefine, TRunCSM, TConfig,
 TRunRaw, TState, TCustomInit, valid_queue, value_to_type} = require 'api'

watch_ran_queues = (old_state, new_state) ->
    for k, v of new_state.q
        if v.length > 0 and v.has_been_ran is true
            debug "Ran queue updated:", k
            run_module AppState, i while i = v.shift()

    new_state

lazy_init_state = (state) ->
    # TODO types?
    state or= {}
    state.q or= {}
    for k, v of CSQS
        state.q[v] or= []
        state.q[v].has_been_ran or= false

    watch_state MY_STATE, watch_ran_queues

    state

# Usage:
# qm = require 'queue-manager'
# qm.schedule(new qm.TRunRaw({modname: 'hz'
#                            ,queue: qm.CSQS.CONFIG_READY_QUEUE
#                            ,raw: function(){ alert(123) }
#                            }))
schedule = (qitem, AppState) ->
    throw new TypeError "Type mismatch for queue item" unless qitem instanceof Type

    swap_state MY_STATE, (old_state) ->
        new_state = lazy_init_state old_state
        new_state.q[qitem.value.queue].push qitem
        new_state


run_queue = (queue_name, runner, qs, AppState) ->
    throw new TypeError "Bad queue" unless valid_queue queue_name

    qs = qs.map value_to_type

    my_state = lazy_init_state (get_state MY_STATE)
    my_q = my_state.q[queue_name]

    # update state asap to prevent race conditions when new items would be
    # added to queue while it is processed
    swap_state MY_STATE, (old_state) ->
        my_state.q[queue_name] = []
        my_state.q[queue_name].has_been_ran = true
        my_state

    external_q = qs.filter (x) -> x.value.queue is queue_name
    full_queue = my_q.concat external_q
    queue_size = full_queue.length
    ran_count = 0
    queue_done = ->
        ran_count = ran_count + 1
        debug "-- Done [#{AppState.realm}:#{queue_name}]" if ran_count is queue_size

    if queue_size > 0
        debug "-- Go [#{AppState.realm}:#{queue_name}]"
        full_queue.map (partial runner, AppState, queue_done)
    else
        debug "-- Empty [#{AppState.realm}:#{queue_name}]"


run_module = (AppState, cont, mod) ->
    setTimeout (-> run_module_worker AppState, cont, mod), 0

run_module_worker = (AppState, cont, mod) ->
    switch typeclass mod # a-la parametric polymorphism
        when TState
            {stateid, key, value} = mod.value
            try
                swap_state key, (old_state) -> value
            catch e
                error "Failed to set state for #{key}/#{stateid}:", e

        when TDefine
            {modname, submodname, modbody} = mod.value
            try
                module = {}
                parsed_mod_body = JSON.parse modbody
                module[submodname] = (exports, require, module) ->
                    module.exports = parsed_mod_body

                # XXX special case
                # local `require` has no `.define()`, must use global one
                window.require.define modname, module
            catch e
                error "Failed to require.define #{modname}/#{submodname}:", e

        when TConfig
            {modname, key, value} = mod.value
            try
                set_config key, value, AppState
            catch e
                error "Failed to set config #{modname}/#{key}:", e

        when TRunCSM
            {modname, args, singleton} = mod.value
            info "Initializing #{AppState.realm}:#{modname} instance"
            sl = require 'service-locator'
            try
                [_..., mod_name] = modname.split '/'
                X = require mod_name
                sl.provide({
                    name: mod_name
                    instance: new X (args.concat [AppState])...
                    singleton: singleton
                }, AppState)
            catch e
                error "Failed to init #{AppState.realm}:#{mod}: #{e}"

        when TRunRaw
            {modname, raw, singleton} = mod.value
            info "Initializing #{AppState.realm}:#{modname} instance with a literal"
            try
                f = if is_function raw
                    raw
                else
                    warn "Usage of string literals in TRunRaw is deprecated!"
                    # XXX eval here - special case
                    new Function (modname.replace /[^A-Za-z0-9]/g, '_'), raw
                f AppState
            catch e
                error "Failed to init #{AppState.realm}:#{modname} with a literal: #{e}"

        when TCustomInit
            {modname, queue, fun} = mod.value
            info "Running custom-init #{AppState.realm}:#{modname}"
            try
                fun AppState
            catch e
                error "Failed running #{AppState.realm}:#{modname} of type custom-init: #{e}"

        else
            throw new TypeError "Unknown type: #{mod}"

    cont true

run_bootstrap_queue     = partial run_queue, CSQS.BOOTSTRAP_QUEUE, run_module
run_define_queue        = partial run_queue, CSQS.DEFINE_QUEUE, run_module
run_init_queue          = partial run_queue, CSQS.INIT_QUEUE, run_module
run_dom_ready_queue     = partial run_queue, CSQS.DOM_READY_QUEUE, run_module
run_doc_load_queue      = partial run_queue, CSQS.DOC_LOAD_QUEUE, run_module
run_config_ready_queue  = partial run_queue, CSQS.CONFIG_READY_QUEUE, run_module


module.exports = { CSQS, CSMTS, value_to_type, run_bootstrap_queue,
    TCustomInit, TDefine, TRunCSM, TRunRaw, TState, schedule,
    run_define_queue, run_init_queue, run_dom_ready_queue,
    run_doc_load_queue, run_config_ready_queue}
