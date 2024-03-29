CREATE OR REPLACE LUA SCRIPT "SQL_DATA_LINEAGE" (sql_text, default_schema) RETURNS TABLE AS
    -- strip double quotes
    function strip_double_quotes(v)
        return string.gsub(v, "^\"(.+)\"$", "%1")
    end

    -- slice a part of array
    function table.slice(tbl, first, last, step)
        local out = {}
        for i = first or 1, last or #tbl, step or 1 do
            out[#out+1] = tbl[i]
        end
        return out
    end

    -- table is empty
    function table.empty(t)
        if t == nil then
            return true
        end

        for _, _ in pairs(t) do
            return false
        end
        return true
    end

    -- merge two tables
    function table.merge(t1, t2)
        if table.empty(t2) then
            return t1
        end

        local t = t1
        for i=1, #t2 do
            t = table.insert_unique(t, t2[i])
        end
        return t
    end

    -- insert unique value
    function table.insert_unique(a, v)
        local a = a
        if not table.has_value(a, v) then
            table.insert(a, v)
        end
        return a
    end

    -- table size
    function table.size(t)
        local count = 0
        for _ in pairs(t) do
            count = count + 1
        end
        return count
    end

    -- check value exists in array
    function table.has_value(a, v)
        for index, value in ipairs(a) do
            if value == v then
                return true
            end
        end
        return false
    end

    -- get table keys
    function table.keys(a)
        local out = {}
        for key, _ in ipairs(a) do
            out[#out+1] = key
        end
        return out
    end

    -- create sorting index by ordinal_position
    function get_table_index(a)
        local out = {}
        if table.empty(a) or a == nil then
            return out
        end

        for k, v in pairs(a) do
            table.insert(out, {name = v.ordinal_position, value = k})
        end
        table.sort(out, function(a,b) return a.name < b.name end)
        return out
    end

    -- get value from table by ordinal_position
    function get_value_by_ordinal_position(a, pos)
        for _, v in pairs(a) do
            if v.ordinal_position == pos then
                return v
            end
        end
    end

    -- get default schema
    function get_default_schema()
        if default_schema ~= null then
            return string.upper(default_schema)
        end
        return exa.meta.current_schema
    end

    -- get columns of exa object
    function get_exa_object_structure(s, o)
        local out = {}
        local success, res = pquery([[
            SELECT
                  COLUMN_SCHEMA
                , COLUMN_TABLE
                , COLUMN_NAME
            FROM EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = :s
                AND COLUMN_TABLE = :o
            ORDER BY COLUMN_ORDINAL_POSITION
        ]], {s = s, o = o})
        if success then
            for i=1, #res do
                out[i] = res[i][3]
            end
        end
        return out
    end

    -- get columns of system object
    function get_system_object_structure(o)
        local out = {}
        local success, res = pquery([[DESC ::o]], {o = o})
        if success then
            for i=1, #res do
                out[i] = res[i][1]
            end
        end
        return out
    end

    -- check if exa object exists
    function is_exa_object(schema_name, object_name)
        local _, res = pquery([[
            SELECT 1
            FROM EXA_ALL_OBJECTS
            WHERE object_name = :o
                AND root_name = :s
            LIMIT 1
        ]], {s = schema_name, o = object_name})
        return (#res>0)
    end

    -- check if system object exists
    function is_system_object(object_name)
        local _, res = pquery([[
            SELECT schema_name
            FROM EXA_SYSCAT
            WHERE object_name = :o
            LIMIT 1
        ]], {o = object_name})
        return (#res>0)
    end

    -- is aggregate function
    function is_agg_func(v)
        if #agg_func_list == 0 then
            local success, res = pquery([[SELECT param_value FROM EXA_METADATA WHERE param_name = 'aggregateFunctions']], {})
            if success then
                for fname in string.gmatch(res[1][1], '([^,]+)') do
                    agg_func_list[#agg_func_list+1] = fname
                end
            end
        end
        return table.has_value(agg_func_list, v)
    end

    -- append existing source
    function append_source(s1, s2, filter)
        local out = s1

        if table.empty(s2) then
            return s1
        elseif table.empty(s1) then
            return s2
        end

        for k, v in pairs(s2) do
            if not(filter == nil) then
                if table.has_value(filter, k) then
                    out[k] = v
                end
            else
                out[k] = v
            end
        end
        return out
    end

    -- create source
    function add_source(tokens, ident, pos, source_map, alias_map, ord)
        local out_source_map = source_map
        local out_alias_map = alias_map
        local pos = pos
        local s, o = parse_identifier(ident)
        local alias_name, alias_pos = get_alias(tokens, pos)
        local cols
        local is_exa_object = is_exa_object(s, o)
        local is_system_object = is_system_object(o)

        if is_system_object then
            s = 'SYS'
        end

        if is_exa_object or is_system_object then
            local key = (alias_name or s .. '.' .. o)

            if is_exa_object then
                cols = get_exa_object_structure(s, o)
            else
                cols = get_system_object_structure(o)
            end

            out_source_map[key] = {columns = {}, ordinal_position = ord}

            for i=1,#cols do
                local name = key .. '.' .. cols[i]
                out_source_map[key].columns[i] = {
                    name = cols[i],
                    source = {table.concat({s, o, cols[i]}, '.')},
                    ordinal_position = i
                }
            end
        else
            if not(alias_name == nil) then
                out_alias_map[alias_name] = o
            end
        end
        return (alias_pos or pos), out_source_map, out_alias_map
    end

    -- remap source
    function remap_source(alias_map, source_map)
        local out = source_map

        if table.empty(alias_map) then
            return out
        end

        -- remaping
        for k, v in pairs(alias_map) do
            if k ~= v then
                out[k] = out[v]
            end
        end

        -- garbage collecting
        for _, v in pairs(alias_map) do
            if not (v == nil) then
                out[v] = nil
            end
        end
        return out
    end

    function isany(token)
        return false
    end

    -- check next token exists
    function next(tokens, pos)
        return not(tokens[pos+1] == nil)
    end

    -- get fake alias
    function get_fake_alias()
        fake_alias_counter = fake_alias_counter + 1
        return '_' .. fake_alias_counter
    end

    -- process context
    function process_context(tokens, source_map)
        local pos = 1
        local source_map = source_map or {}
        local out_source_map = {}
        local cte_source_map = {}
        local from_source_map = {}
        local alias_map = {}

        if is_union_statement(tokens) then
            return process_union(tokens, source_map)
        end

        -- discover CTE
        pos, cte_source_map = process_with(tokens, source_map)

        -- append sources from CTE to in source
        source_map = append_source(source_map, cte_source_map)

        -- get column expr
        local column_expr, pos = process_select(tokens, pos)

        if pos == nil then
            return nil, nil
        end

        -- process from
        pos, from_source_map, alias_map = process_from(tokens, pos, source_map)

        -- put discovered sources into a context
        out_source_map = append_source(out_source_map, from_source_map)

        -- remap out_source_map if there is a link to CTE
        out_source_map = remap_source(alias_map, out_source_map)

        -- process joins
        pos, join_source_map, alias_map = process_join(tokens, pos, source_map)

        -- filter out sources
        out_source_map = from_source_map
        out_source_map = append_source(out_source_map, join_source_map)

        -- remap out_source_map if there is a link to CTE
        out_source_map = remap_source(alias_map, out_source_map)

        local filter = table.merge(table.keys(from_source_map), table.keys(join_source_map))
        out_source_map = append_source(out_source_map, cte_source_map, filter)

        return process_columns(tokens, column_expr, out_source_map)
    end

    -- process with statement
    function process_with(tokens, source_map)
        local pos = 1
        local source_map = source_map or {}
        local out_source_map = {}
        local with = sqlparsing.find(tokens, pos, true, true, isany, 'WITH')

        if with == nil then
            return pos, nil
        end

        pos = with[1]

        repeat
            local name, start_pos, end_pos = get_cte(tokens, pos)

            if name == nil then
                break
            end

            pos = end_pos

            local source = process_context(table.slice(tokens, start_pos+1, end_pos-1), source_map)
            source_map[name] = {columns = source}
            out_source_map[name] = {columns = source}
        until name == nil
        return pos, out_source_map
    end

    -- detect CTE
    function get_cte(tokens, pos)
        local pos = pos
        local cte_start = sqlparsing.find(tokens, pos+1, true, true, isany, sqlparsing.isidentifier, 'AS', '(')
        if cte_start == nil then
            return nil, nil, nil
        end

        local cte_end = sqlparsing.find(tokens, cte_start[3], true, true, isany, ')')
        if cte_end == nil then
            error('Missing closing bracket for CTE')
        end
        return tokens[cte_start[1]], cte_start[3], cte_end[1]
    end

    -- process select
    function process_select(tokens, pos)
        local pos = pos

        local select = sqlparsing.find(tokens, pos, true, false, isany, 'SELECT')
        if select == nil then
            return {}
        end

        pos = select[1]

        local distinct = sqlparsing.find(tokens, pos+1, true, true, isany, 'DISTINCT')
        if not(distinct == nil) then
            pos = distinct[1]
        end

        local end_pos = #tokens
        local from = sqlparsing.find(tokens, pos+1, true, true, isany, 'FROM')
        if not(from == nil) then
            end_pos = from[1]-1
        end
        return get_columns_expr(tokens, pos+1, end_pos), from and from[1] or #tokens
    end

    -- check is union statement
    function is_union_statement(tokens)
        local union = sqlparsing.find(tokens, 1, true, true, isany, 'UNION')
        return not(union == nil)
    end

    -- check is emit function
    function is_emits_keyword(tokens)
        local res = sqlparsing.find(tokens, 1, true, true, isany, 'EMITS')
        return not(res == nil)
    end

    -- process union statements
    function process_union(tokens, source_map)
        local out = {}
        local source_map = source_map or {}
        local queries = get_union_queries(tokens)

        if table.empty(queries) then
            error('Union queries not found')
        end

        for i=1, #queries do
            local columns = process_context(queries[i], source_map)
            if i == 1 then
                out = columns
            else
                if not table.empty(columns) then
                    for _, line in ipairs(get_table_index(out)) do
                        local k = line.value
                        local v = get_value_by_ordinal_position(columns, out[k].ordinal_position)
                        if not(v == nil) then
                            out[k].source = table.merge(out[k].source, v.source)
                            out[k].fname = table.merge(out[k].fname or {}, v.fname or {})
                        end
                    end
                end
            end
        end
        return out
    end

    -- get all queries from union
    function get_union_queries(tokens)
        local out = {}
        local pos = 1

        local found
        repeat
            local union = sqlparsing.find(tokens, pos, true, true, isany, 'UNION')
            if union == nil then
                found = false
                break
            end

            out[#out+1] = get_context_from_tokens(table.slice(tokens, pos, union[1] - 1))
            pos = union[1] + 1
        until found == false

        out[#out+1] = get_context_from_tokens(table.slice(tokens, pos, #tokens))
        return out
    end

    function get_context_from_tokens(tokens)
        local tokens = tokens
        for i=1, #tokens do
            if tokens[i] == 'ALL' then
            elseif tokens[i] == '(' then
                local closing_bracket = sqlparsing.find(tokens, i, true, true, isany, ')')
                if closing_bracket == nil then
                    error('Closing bracket for subquery not found')
                end
                return get_context_from_tokens(table.slice(tokens, i+1, closing_bracket[1]-1))
            else
                return table.slice(tokens, i, #tokens)
            end
        end
    end

    -- process a single from
    function process_from(tokens, pos, source_map)
        local pos = pos
        local source_map = source_map or {}
        local out_source_map = {}
        local alias_map = {}
        local ord = 0

        -- find from
        local from = sqlparsing.find(tokens, pos, true, true, isany, 'FROM')
        if not(from == nil) and next(tokens, from[1]) then
            -- subquery
            if tokens[from[1]+1] == '(' then
                local pos, columns = get_columns_from_subquery(tokens, from[1]+1, source_map)
                if not(columns == nil) then
                    local alias_name, alias_pos = get_alias(tokens, pos)
                    if alias_name == nil then
                        ord = ord+1
                        out_source_map[get_fake_alias()] = {columns = columns, ordinal_position = ord}
                        return (alias_pos or pos), out_source_map
                    end

                    pos = alias_pos
                    ord = ord+1
                    out_source_map[alias_name] = {columns = columns, ordinal_position = ord}
                end
            elseif sqlparsing.isidentifier(tokens[from[1]+1]) then
                local from_ident = tokens[from[1]+1]
                if not(source_map[from_ident] == nil) then
                    local alias_name, alias_pos = get_alias(tokens, from[1]+1)
                    ord = ord+1
                    out_source_map[(alias_name or from_ident)] = source_map[from_ident]
                    out_source_map[(alias_name or from_ident)].ordinal_position = ord
                    pos = alias_pos or from[1]+1
                else
                    pos, out_source_map, alias_map = add_source(tokens, from_ident, from[1]+1, out_source_map, alias_map, ord)
                end
            end
        end

        -- find objects joined by comma
        local tmp_pos = pos
        local quit = false
        repeat
            if next(tokens, tmp_pos) then
                tmp_pos = tmp_pos+1
            else
                quit = true
                break
            end

            if tokens[tmp_pos] ~= ',' then
                quit = true
                break
            end

            if next(tokens, tmp_pos) then
                tmp_pos = tmp_pos+1
            else
                quit = true
                break
            end

            if not sqlparsing.isidentifier(tokens[tmp_pos]) then
                quit = true
                break
            end

            local ident = tokens[tmp_pos]
            if not(source_map[ident] == nil) then
                ord = ord+1
                out_source_map[ident] = source_map[ident]
                out_source_map[ident].ordinal_position = ord
                pos = tmp_pos
            else
                ord = ord+1
                pos, out_source_map, alias_map = add_source(tokens, ident, tmp_pos, out_source_map, alias_map, ord)
            end
        until quit == false
        return (alias_pos or pos), out_source_map, alias_map
    end

    -- process join statements
    function process_join(tokens, pos, source_map)
        local pos = pos
        local out_source_map = {}
        local alias_map = {}
        local ord = 0

        if not next(tokens, pos) then
            return pos, out_source_map, alias_map
        end

        -- process regular joins
        local found
        repeat
            local join = sqlparsing.find(tokens, pos+1, true, true, isany, 'JOIN')
            if join == nil then
                found = false
                break
            end

            pos = pos+1

            -- join subquery
            if tokens[join[1]+1] == '(' then
                local pos, columns = get_columns_from_subquery(tokens, join[1]+1, source_map)
                if not table.empty(columns) then
                    local alias_name, alias_pos = get_alias(tokens, pos)
                    if alias_name == nil then
                        ord = ord+1
                        out_source_map[get_fake_alias()] = {columns = columns, ordinal_position = ord}
                        return (alias_pos or pos), out_source_map
                    end

                    ord = ord+1
                    out_source_map[alias_name] = {columns = columns, ordinal_position = ord}
                    pos = alias_pos
                end
            -- join regular exa object
            elseif sqlparsing.isidentifier(tokens[join[1]+1]) then
                local ident = tokens[join[1]+1]
                local alias_name, alias_pos = get_alias(tokens, join[1]+1)
                pos = alias_pos or pos
                if source_map[ident] or source_map[alias_name] then
                    ord = ord+1
                    local source = {}

                    if not table.empty(source_map[ident]) then
                        source = source_map[ident]
                    elseif not table.empty(source_map[alias_name]) then
                        source = source_map[alias_name]
                    end

                    out_source_map[ident] = {columns = source.columns, ordinal_position = ord}
                    if not(alias_name == nil) then
                        alias_map[alias_name] = ident
                    end
                    pos = alias_pos or join[1]+1
                else
                    ord = ord+1
                    pos, out_source_map, alias_map = add_source(tokens, ident, join[1]+1, out_source_map, alias_map, ord)
                end
            end
        until found == false
        return (alias_pos or pos), out_source_map, alias_map
    end

    -- combine column expressions with sources
    function process_columns(tokens, column_expr, source_map)
        local out = {}
        local ord = 0
        local source_map = source_map

        for i=1,#column_expr do
            local target_column_expr = column_expr[i]
            local source_object_alias, target_name, target_alias, column_type = parse_column_expr(target_column_expr)
            target_name = strip_double_quotes(target_name)
            if target_name == '*' then -- expand *
                if source_object_alias == nil then     -- get all columns from source_map
                    for _, object_line in ipairs(get_table_index(source_map)) do
                        local object_alias = object_line.value
                        local object_meta = source_map[object_alias]
                        for _, line in ipairs(get_table_index(object_meta.columns)) do
                            local key = line.value
                            local meta = object_meta.columns[key]
                            ord = ord + 1
                            out[#out+1] = {name = object_meta.columns[key].name, source = meta.source, column_type = column_type, ordinal_position = ord}
                        end
                    end
                else
                    if source_map[source_object_alias] == nil then
                        error('Source ' .. source_object_alias .. ' not found in map')
                    end

                    for _, line in ipairs(get_table_index(source_map[source_object_alias].columns)) do
                        local key = line.value
                        local meta = source_map[source_object_alias].columns[key]
                        ord = ord + 1
                        out[#out+1] = {name = source_map[source_object_alias].columns[key].name, source = meta.source, column_type = column_type, ordinal_position = ord}
                    end
                end
            elseif column_type == 'common' then
                ord = ord + 1
                if source_object_alias == 'LOCAL' then
                    local local_column = get_column_by_name(target_name, out)
                    if not(local_column == nil) then
                        out[#out+1] = {name = target_alias, source = local_column.source, column_type = column_type, ordinal_position = ord}
                    end
                else
                    local meta = get_column_from_source(target_name, source_object_alias, source_map)

                    local source = {}
                    if meta and meta.source then
                        source = meta.source
                    end

                    local fname = {}
                    if meta and meta.fname then
                        fname = meta.fname
                    end

                    local is_agg = false
                    if meta and meta.is_agg then
                        is_agg = meta.is_agg
                    end
                    out[#out+1] = {name = target_alias or target_name, source = source, column_type = column_type, fname = fname, is_agg = is_agg, ordinal_position = ord}
                end
            else
                local found_columns = 0
                local exp_tokens = sqlparsing.tokenize(target_name)
                local names = {}

                if is_emits_keyword(exp_tokens) then
                    names, target_name = parse_emitting_func(exp_tokens)
                else
                    if target_alias == nil then
                        target_alias = target_name
                    end
                    names[#names+1] = target_alias
                end

                local expr_columns = extract_columns_from_expression(target_name)

                if not table.empty(expr_columns) then
                    local row = {source = {}, column_type = column_type}
                    for k, v in pairs(expr_columns) do
                        local tmp_alias, tmp_name, _, _ = parse_column_expr(k)
                        tmp_name = strip_double_quotes(tmp_name)
                        local meta

                        if tmp_alias == 'LOCAL' then
                            local local_column = get_column_by_name(tmp_name, out)
                            if not(local_column == nil) then
                                meta = {source = local_column.source, fname = local_column.fname}
                            end
                        else
                            meta = get_column_from_source(tmp_name, tmp_alias, source_map) or {}
                        end

                        if row.fname == nil then
                            row.fname = {v.fname}
                            row.is_agg = v.is_agg
                            row.is_dist = v.is_dist
                        end

                        if not table.empty(meta) then
                            row.source = table.merge(row.source, meta.source)
                            row.fname = table.merge(row.fname, meta.fname)
                            row.is_agg = row.is_agg or v.is_agg
                            row.is_dist = row.is_dist or v.is_dist
                            found_columns = found_columns+1
                        end
                    end

                    for z=1,#names do
                        ord = ord + 1
                        out[#out+1] = {name = names[z], source = row.source, column_type = row.column_type, ordinal_position = ord}
                    end
                else
                    ord = ord + 1
                    out[#out+1] = {name = target_alias or target_name, source = {}, column_type = column_type, ordinal_position = ord}
                end
            end
        end
        return out
    end

    function extract_columns_from_expression(expr)
        local tokens = sqlparsing.tokenize(expr)
        local out = {}
        for i=1,#tokens do
            if sqlparsing.isidentifier(tokens[i]) and not sqlparsing.iskeyword(tokens[i]) then
                local is_agg = false
                local is_dist = false
                local fname
                local func = sqlparsing.find(tokens, i-1, false, false, isany, sqlparsing.iskeyword, '(')

                if not(func == nil) then
                    fname = tokens[func[1]]
                    is_agg = is_agg_func(tokens[func[1]])

                    if is_agg then
                        local pos = i
                        repeat
                            pos = pos-1
                            if tokens[pos] == 'DISTINCT' then
                                is_dist = true
                                break
                            else
                                break
                            end
                        until pos == func[2]
                    end
                end
                out[strip_double_quotes(tokens[i])] = {fname = fname, is_agg = is_agg, is_dist = is_dist}
            end
        end
        return out
    end

    -- get column by name
    function get_column_by_name(name, a)
        for i=1, #a do
            if a[i].name == name then
                return a[i]
            end
        end
    end

    -- get column data from source
    function get_column_from_source(target_name, object_alias, source_map)
        local out
        if object_alias == nil then
            for a, object_meta in pairs(source_map) do
                if object_meta.columns then
                    out = get_column_by_name(target_name, object_meta.columns)
                    if not(out == nil) then
                        return out
                    end
                end
            end
        else
            -- if column alias is an object name
            if source_map[object_alias] == nil then
                for key, _ in pairs(source_map) do
                    local s, o = parse_identifier(key)
                    if not(s == nil) and o == object_alias then
                        object_alias = key
                        break
                    end
                end
            end

            if not(source_map[object_alias] == nil) and source_map[object_alias].columns then
                return get_column_by_name(target_name, source_map[object_alias].columns)
            end
        end
    end

    -- get columns from emitting function
    function parse_emitting_func(tokens)
        local columns = {}
        local func_expr

        -- emits starts
        local emits_start = sqlparsing.find(tokens, 1, true, true, isany, 'EMITS', '(')
        if emits_start == nil then
            return
        end

        -- emits ends
        local emits_end = sqlparsing.find(tokens, emits_start[2], true, true, isany, ')')
        if emits_end == nil then
            return
        end

        emits_args_tokens = table.slice(tokens, emits_start[2]+1, emits_end[1]-1)
        for i=1,#emits_args_tokens do
            if sqlparsing.isidentifier(emits_args_tokens[i]) and not sqlparsing.iskeyword(emits_args_tokens[i]) then
                columns[#columns+1] = strip_double_quotes(emits_args_tokens[i])
            end
        end

        -- function starts
        local func_start = sqlparsing.find(tokens, emits_start[1]-1, false, true, isany, sqlparsing.isidentifier, '(')
        if func_start == nil then
            return
        end

        -- function ends
        local func_end = sqlparsing.find(tokens, func_start[2], true, true, isany, ')')
        if func_end == nil then
            return
        end
        return columns, table.concat(table.slice(tokens, func_start[1], func_end[1]))
    end

    -- get columns data from subquery
    function get_columns_from_subquery(tokens, pos, source_map)
        local closing_brackets = sqlparsing.find(tokens, pos, true, true, isany, ')')
        if closing_brackets == nil then
            error('Closing bracket for subquery not found')
        end
        return closing_brackets[1], process_context(table.slice(tokens, pos+1, closing_brackets[1]-1), source_map)
    end

    -- discover alias
    function get_alias(tokens, pos)
        local pos = pos
        -- skip AS
        if not(tokens[pos+1] == nil) and tokens[pos+1] == 'AS' then
            pos = pos+1
        end

        -- alias
        if not(tokens[pos+1] == nil) and sqlparsing.isidentifier(tokens[pos+1]) then
            pos = pos+1
            return tokens[pos], pos
        end
        return nil, nil
    end

    -- return column expression between select and from
    function get_columns_expr(tokens, start_pos, end_pos)
        local column_tokens = table.slice(tokens, start_pos, end_pos)
        local columns = {}
        local i = 1
        local br = 1

        while (#column_tokens > i) do
            if column_tokens[i] == ',' then
                columns[#columns+1] = format_column_expression(table.slice(column_tokens, br, i-1))
                br = i+1
            elseif column_tokens[i] == '(' then
                local closing_bracket = sqlparsing.find(column_tokens, i, true, true, isany, ')')
                if closing_bracket == nil then
                    error('Closing bracket not found')
                end
                i = closing_bracket[1]
            end
            i = i+1
        end
        columns[#columns+1] = format_column_expression(table.slice(column_tokens, br, #column_tokens))
        return columns
    end

    -- format column expression
    function format_column_expression(tokens)
        local out = {}
        for i=1,#tokens do
            if i>1 then
                if table.has_value({'(', '.'}, tokens[i-1]) then
                elseif table.has_value({'(', ')', ',', '.'}, tokens[i]) then
                else
                    out[#out+1] = ' '
                end
            end
            out[#out+1] = tokens[i]
        end
        return table.concat(out)
    end

    -- parse column expression
    function parse_column_expr(v)
        local v = v
        local column_alias

        if string.match(v, '^.+%s+(AS%s+)[%w_]+$') then
            v, column_alias = string.match(v, '^(.+)%s+AS%s+([%w_]+)$')
        elseif string.match(v, '^.+%s+(AS%s+)"[%w_]+"$') then
            v, column_alias = string.match(v, '^(.+)%s+AS%s+"([%w_]+)"$')
        elseif string.match(v, '^.+%s+[%w_]+$') then
            v, column_alias = string.match(v, '^(.+)%s+([%w_]+)$')
        end

        -- detect colnames
        if string.match(v, '%*$') then
            return string.match(v, "^(.+)%."), '*', column_alias, 'wildcard'
        end

        if string.match(v, '^[%w_]+%.[%w_]+$') then
            local alias, name = string.match(v, '^([%w_]+)%.([%w_]+)$')
            return alias, name, column_alias, 'common'
        elseif string.match(v, '^[%w_]+%."[%w_]+"$') then
            local alias, name = string.match(v, '^([%w_]+)%.("[%w_]+")$')
            return alias, name, column_alias, 'common'
        end

        -- e.g. "1 as is_smth_fl"
        local tmp_tokens = sqlparsing.tokenize(v)
        if #tmp_tokens>1 or not(sqlparsing.isidentifier(v)) then
            return nil, v, column_alias, 'expression'
        end

        if string.match(v, '^[%w_]+$') then
            return alias, v, column_alias, 'common'
        end

        return nil, v, column_alias, 'expression'
    end

    -- parse identifier
    function parse_identifier(identifier)
        local s, o = string.match(identifier, '^([%w_]+)%.([%w_]+)$')
        return s or default_schema_name, o or identifier
    end

    -- remove spaces and comments
    function normalize_tokens(tokens)
        local out = {}
        for i=1, #tokens do
            if not sqlparsing.iswhitespaceorcomment(tokens[i]) then
                if sqlparsing.iskeyword(tokens[i]) or sqlparsing.isidentifier(tokens[i]) then
                    out[#out+1] = string.upper(tokens[i])
                else
                    out[#out+1] = tokens[i]
                end
            end
        end
        return out
    end

    -- remove garbage part from sql
    function get_main_query(tokens)
        local tokens = tokens
        local pos = 0
        local pos_end = #tokens

        -- process create
        local token = sqlparsing.find(tokens, pos+1, true, true, isany, 'CREATE')
        if not(token == nil) then
            pos = token[1]
            repeat
                pos = pos+1

                if pos > #tokens then
                    local found = false
                    break
                end

                if tokens[pos] == 'AS' then
                    local found = false
                    break
                end
            until found == false
        end

        -- check opening round bracket right after
        repeat
            pos = pos+1

            if pos > #tokens then
                local found = false
                break
            end

            if tokens[pos] == '(' then
                -- find closing bracket
                local token = sqlparsing.find(tokens, pos, true, true, isany, ')')

                if token == nil then
                    error('Closing round bracket not found')
                end

                local found = false
                pos = pos+1
                pos_end = token[1]-1
                break
            else
                local found = false
                break
            end
        until found == false
        return table.slice(tokens, pos, pos_end)
    end

    -- print result
    function show_result(columns)
        local titles = "column_name varchar(128), source_schema_name varchar(64), source_object_name varchar(64), source_column_name varchar(64), fname varchar(128), is_agg boolean, ordinal_position decimal(3,0)"
        local rows = {}

        for _, line in ipairs(get_table_index(columns)) do
            local k = line.value
            local v = columns[k]

            local fname
            if not table.empty(v.fname) then
                fname = table.concat(v.fname, ',')
            end

            if table.empty(v.source) then
                rows[#rows+1] = {v.name, nil, nil, nil, fname, v.is_agg or false, v.ordinal_position}
            else
                for i=1, #v.source do
                    local schema_name, object_name, column_name = string.match(v.source[i], '^(.+)%.(.+)%.(.+)$')
                    rows[#rows+1] = {
                        string.sub(v.name, 1, 128),
                        schema_name,
                        object_name,
                        column_name,
                        fname,
                        v.is_agg or false,
                        v.ordinal_position
                    }
                end
            end
        end
        exit(rows, titles)
    end

    -- aggregate functions list
    agg_func_list = {}

    -- schema name
    default_schema_name = get_default_schema()
    if default_schema_name == nil then
        error('Default schema is not defined.')
    end

    -- counter for fake aliases
    fake_alias_counter = 0

    local tokens = normalize_tokens(sqlparsing.tokenize(sql_text))
    tokens = get_main_query(tokens)
    show_result(process_context(tokens, nil))
/