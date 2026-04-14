import radb
import radb.ast


def rule_break_up_selections(ra_expr):

    if isinstance(ra_expr, radb.ast.Select):
        condition = ra_expr.cond
        input_expr = rule_break_up_selections(ra_expr.inputs[0])
        
        # Check if condition is a conjunction (AND)
        if isinstance(condition, radb.ast.ValExprBinaryOp) and condition.op == radb.ast.sym.AND:
            # Break up the conjunction
            conditions = []
            collect_and_conditions(condition, conditions)
            
            # Apply selections from innermost to outermost
            result = input_expr
            for cond in reversed(conditions):
                result = radb.ast.Select(cond, result)
            return result
        else:
            return radb.ast.Select(condition, input_expr)
    
    elif isinstance(ra_expr, radb.ast.Project):
        return radb.ast.Project(ra_expr.attrs, rule_break_up_selections(ra_expr.inputs[0]))
    
    elif isinstance(ra_expr, radb.ast.Rename):
        return radb.ast.Rename(ra_expr.relname, ra_expr.attrnames, rule_break_up_selections(ra_expr.inputs[0]))
    
    elif isinstance(ra_expr, radb.ast.Join):
        left = rule_break_up_selections(ra_expr.inputs[0])
        right = rule_break_up_selections(ra_expr.inputs[1])
        return radb.ast.Join(left, ra_expr.cond, right)
    
    elif isinstance(ra_expr, radb.ast.Cross):
        left = rule_break_up_selections(ra_expr.inputs[0])
        right = rule_break_up_selections(ra_expr.inputs[1])
        return radb.ast.Cross(left, right)
    
    else:
        return ra_expr


def collect_and_conditions(condition, conditions):
    """Helper function to collect all conditions in an AND expression."""
    if isinstance(condition, radb.ast.ValExprBinaryOp) and condition.op == radb.ast.sym.AND:
        for inp in condition.inputs:
            collect_and_conditions(inp, conditions)
    else:
        conditions.append(condition)


def rule_push_down_selections(ra_expr, dd):

    if isinstance(ra_expr, radb.ast.Select):
        condition = ra_expr.cond
        input_expr = ra_expr.inputs[0]
        
        processed_input = rule_push_down_selections(input_expr, dd)
        
        
        if isinstance(processed_input, radb.ast.Select) and isinstance(processed_input.inputs[0], radb.ast.Cross):
            inner_select = processed_input
            cross = inner_select.inputs[0]
            
            left = cross.inputs[0]
            right = cross.inputs[1]
            
            left_attrs = get_relation_attrs(left, dd)
            right_attrs = get_relation_attrs(right, dd)
            condition_attrs = get_condition_attrs(condition)
            
            if condition_attrs.issubset(left_attrs):
                new_cross = radb.ast.Cross(radb.ast.Select(condition, left), right)
                return radb.ast.Select(inner_select.cond, new_cross)
            elif condition_attrs.issubset(right_attrs):
                new_cross = radb.ast.Cross(left, radb.ast.Select(condition, right))
                return radb.ast.Select(inner_select.cond, new_cross)
            else:
                return radb.ast.Select(condition, processed_input)
        
        elif isinstance(processed_input, radb.ast.Cross):
            left = processed_input.inputs[0]
            right = processed_input.inputs[1]
            
            left_attrs = get_relation_attrs(left, dd)
            right_attrs = get_relation_attrs(right, dd)
            condition_attrs = get_condition_attrs(condition)
            
            if condition_attrs.issubset(left_attrs):
                # Recursively push down to the left
                pushed_left = rule_push_down_selections(radb.ast.Select(condition, left), dd)
                return radb.ast.Cross(pushed_left, right)
            elif condition_attrs.issubset(right_attrs):
                # Recursively push down to the right
                pushed_right = rule_push_down_selections(radb.ast.Select(condition, right), dd)
                return radb.ast.Cross(left, pushed_right)
            else:
                return radb.ast.Select(condition, processed_input)
        else:
            return radb.ast.Select(condition, processed_input)
    
    elif isinstance(ra_expr, radb.ast.Project):
        return radb.ast.Project(ra_expr.attrs, rule_push_down_selections(ra_expr.inputs[0], dd))
    
    elif isinstance(ra_expr, radb.ast.Rename):
        return radb.ast.Rename(ra_expr.relname, ra_expr.attrnames, rule_push_down_selections(ra_expr.inputs[0], dd))
    
    elif isinstance(ra_expr, radb.ast.Cross):
        left = rule_push_down_selections(ra_expr.inputs[0], dd)
        right = rule_push_down_selections(ra_expr.inputs[1], dd)
        return radb.ast.Cross(left, right)
    
    elif isinstance(ra_expr, radb.ast.Join):
        left = rule_push_down_selections(ra_expr.inputs[0], dd)
        right = rule_push_down_selections(ra_expr.inputs[1], dd)
        return radb.ast.Join(left, ra_expr.cond, right)
    
    else:
        return ra_expr


def rule_merge_selections(ra_expr):

    if isinstance(ra_expr, radb.ast.Select):
        condition = ra_expr.cond
        input_expr = ra_expr.inputs[0]
        
        merged_input = rule_merge_selections(input_expr)
        
        if isinstance(merged_input, radb.ast.Select):
            inner_condition = merged_input.cond
            inner_input = merged_input.inputs[0]
            
            # To build left-associative ((A AND B) AND C), we need to:
            # If inner is already (X AND Y), make it ((condition AND X) AND Y)
            # Otherwise just (condition AND inner)
            if isinstance(inner_condition, radb.ast.ValExprBinaryOp) and inner_condition.op == radb.ast.sym.AND:
                # Inner is (X AND Y), rebuild as ((condition AND X) AND Y)
                new_left = radb.ast.ValExprBinaryOp(condition, radb.ast.sym.AND, inner_condition.inputs[0])
                merged_condition = radb.ast.ValExprBinaryOp(new_left, radb.ast.sym.AND, inner_condition.inputs[1])
            else:
                # Inner is simple, just (condition AND inner)
                merged_condition = radb.ast.ValExprBinaryOp(condition, radb.ast.sym.AND, inner_condition)
            
            return radb.ast.Select(merged_condition, inner_input)
        else:
            return radb.ast.Select(condition, merged_input)
    
    elif isinstance(ra_expr, radb.ast.Project):
        return radb.ast.Project(ra_expr.attrs, rule_merge_selections(ra_expr.inputs[0]))
    
    elif isinstance(ra_expr, radb.ast.Rename):
        return radb.ast.Rename(ra_expr.relname, ra_expr.attrnames, rule_merge_selections(ra_expr.inputs[0]))
    
    elif isinstance(ra_expr, radb.ast.Cross):
        left = rule_merge_selections(ra_expr.inputs[0])
        right = rule_merge_selections(ra_expr.inputs[1])
        return radb.ast.Cross(left, right)
    
    elif isinstance(ra_expr, radb.ast.Join):
        left = rule_merge_selections(ra_expr.inputs[0])
        right = rule_merge_selections(ra_expr.inputs[1])
        return radb.ast.Join(left, ra_expr.cond, right)
    
    else:
        return ra_expr


def rule_introduce_joins(ra_expr):
   
    if isinstance(ra_expr, radb.ast.Select):
        condition = ra_expr.cond
        input_expr = ra_expr.inputs[0]
        
        processed_input = rule_introduce_joins(input_expr)
        
        if isinstance(processed_input, radb.ast.Cross):
            left = processed_input.inputs[0]
            right = processed_input.inputs[1]
            
            if is_join_condition(condition):
                return radb.ast.Join(left, condition, right)
            else:
                return radb.ast.Select(condition, processed_input)
        else:
            return radb.ast.Select(condition, processed_input)
    
    elif isinstance(ra_expr, radb.ast.Project):
        return radb.ast.Project(ra_expr.attrs, rule_introduce_joins(ra_expr.inputs[0]))
    
    elif isinstance(ra_expr, radb.ast.Rename):
        return radb.ast.Rename(ra_expr.relname, ra_expr.attrnames, rule_introduce_joins(ra_expr.inputs[0]))
    
    elif isinstance(ra_expr, radb.ast.Cross):
        left = rule_introduce_joins(ra_expr.inputs[0])
        right = rule_introduce_joins(ra_expr.inputs[1])
        return radb.ast.Cross(left, right)
    
    elif isinstance(ra_expr, radb.ast.Join):
        left = rule_introduce_joins(ra_expr.inputs[0])
        right = rule_introduce_joins(ra_expr.inputs[1])
        return radb.ast.Join(left, ra_expr.cond, right)
    
    else:
        return ra_expr


def is_join_condition(condition):
    if isinstance(condition, radb.ast.ValExprBinaryOp):
        if condition.op == radb.ast.sym.EQ:
            left_expr = condition.inputs[0]
            right_expr = condition.inputs[1]
            
            if isinstance(left_expr, radb.ast.AttrRef) and isinstance(right_expr, radb.ast.AttrRef):
                return True
        elif condition.op == radb.ast.sym.AND:
            # All sub-conditions must be join conditions
            return all(is_join_condition(inp) for inp in condition.inputs)
    
    return False


def get_relation_attrs(ra_expr, dd):
    if isinstance(ra_expr, radb.ast.RelRef):
        if ra_expr.rel in dd:
            attrs = set()
            for attr in dd[ra_expr.rel].keys():
                attrs.add(ra_expr.rel + '.' + attr)
                attrs.add(attr)
            return attrs
        return set()
    
    elif isinstance(ra_expr, radb.ast.Select):
        return get_relation_attrs(ra_expr.inputs[0], dd)
    
    elif isinstance(ra_expr, radb.ast.Project):
        attrs = set()
        for attr in ra_expr.attrs:
            attrs.add(str(attr))
            attr_str = str(attr)
            if '.' in attr_str:
                unqualified = attr_str.split('.', 1)[1]
                attrs.add(unqualified)
        return attrs
    
    elif isinstance(ra_expr, radb.ast.Rename):
        input_attrs = get_relation_attrs(ra_expr.inputs[0], dd)
        
        new_rel = ra_expr.relname
        
        if new_rel is None:
            return input_attrs
        
        if isinstance(ra_expr.inputs[0], radb.ast.RelRef):
            old_rel = ra_expr.inputs[0].rel
            
            renamed_attrs = set()
            for attr in input_attrs:
                if attr.startswith(old_rel + '.'):
                    # Replace old relation name with new
                    attr_name = attr.split('.', 1)[1]
                    renamed_attrs.add(new_rel + '.' + attr_name)
                    renamed_attrs.add(attr_name)
                elif '.' not in attr:
                    # Unqualified attribute
                    renamed_attrs.add(new_rel + '.' + attr)
                    renamed_attrs.add(attr)
                else:
                    # Keep as-is if it doesn't match
                    renamed_attrs.add(attr)
            return renamed_attrs
        else:
            renamed_attrs = set()
            for attr in input_attrs:
                if '.' in attr:
                    attr_name = attr.split('.', 1)[1]
                else:
                    attr_name = attr
                renamed_attrs.add(new_rel + '.' + attr_name)
                renamed_attrs.add(attr_name)
            return renamed_attrs
    
    elif isinstance(ra_expr, radb.ast.Cross) or isinstance(ra_expr, radb.ast.Join):
        left_attrs = get_relation_attrs(ra_expr.inputs[0], dd)
        right_attrs = get_relation_attrs(ra_expr.inputs[1], dd)
        return left_attrs.union(right_attrs)
    
    return set()


def get_condition_attrs(condition):
    attrs = set()
    
    if isinstance(condition, radb.ast.AttrRef):
        attr_str = str(condition)
        attrs.add(attr_str)
        if '.' in attr_str:
            unqualified = attr_str.split('.', 1)[1]
            attrs.add(unqualified)
    elif isinstance(condition, radb.ast.ValExprBinaryOp):
        for inp in condition.inputs:
            attrs.update(get_condition_attrs(inp))
    
    return attrs


def rule_push_down_projections(ra_expr, dd):
    """
    Push projections down past joins to reduce join input sizes.
    Combines with existing Select operations to avoid adding extra MR jobs.
    """
    if isinstance(ra_expr, radb.ast.Project):
        input_expr = ra_expr.inputs[0]
        
        # Try to push projection through joins
        if isinstance(input_expr, radb.ast.Join):
            needed_attrs = set(str(attr) for attr in ra_expr.attrs)
            join_attrs = get_condition_attrs(input_expr.cond)
            all_needed = needed_attrs.union(join_attrs)
            
            left_attrs_available = get_relation_attrs(input_expr.inputs[0], dd)
            right_attrs_available = get_relation_attrs(input_expr.inputs[1], dd)
            
            left_needed = set()
            right_needed = set()
            
            for attr in all_needed:
                if attr in left_attrs_available:
                    left_needed.add(attr)
                if attr in right_attrs_available:
                    right_needed.add(attr)
            
            # Create projections on join inputs if they reduce attributes
            left_optimized = _maybe_add_projection(input_expr.inputs[0], left_needed, left_attrs_available, dd)
            right_optimized = _maybe_add_projection(input_expr.inputs[1], right_needed, right_attrs_available, dd)
            
            new_join = radb.ast.Join(left_optimized, input_expr.cond, right_optimized)
            return radb.ast.Project(ra_expr.attrs, new_join)
        
        elif isinstance(input_expr, radb.ast.Cross):
            needed_attrs = set(str(attr) for attr in ra_expr.attrs)
            
            left_attrs_available = get_relation_attrs(input_expr.inputs[0], dd)
            right_attrs_available = get_relation_attrs(input_expr.inputs[1], dd)
            
            left_needed = set()
            right_needed = set()
            
            for attr in needed_attrs:
                if attr in left_attrs_available:
                    left_needed.add(attr)
                if attr in right_attrs_available:
                    right_needed.add(attr)
            
            # Create projections on cross product inputs if they reduce attributes
            left_optimized = _maybe_add_projection(input_expr.inputs[0], left_needed, left_attrs_available, dd)
            right_optimized = _maybe_add_projection(input_expr.inputs[1], right_needed, right_attrs_available, dd)
            
            new_cross = radb.ast.Cross(left_optimized, right_optimized)
            return radb.ast.Project(ra_expr.attrs, new_cross)
        
        else:
            return radb.ast.Project(ra_expr.attrs, rule_push_down_projections(input_expr, dd))
    
    elif isinstance(ra_expr, radb.ast.Select):
        return radb.ast.Select(ra_expr.cond, rule_push_down_projections(ra_expr.inputs[0], dd))
    
    elif isinstance(ra_expr, radb.ast.Rename):
        return radb.ast.Rename(ra_expr.relname, ra_expr.attrnames, 
                              rule_push_down_projections(ra_expr.inputs[0], dd))
    
    elif isinstance(ra_expr, radb.ast.Join):
        return radb.ast.Join(rule_push_down_projections(ra_expr.inputs[0], dd), 
                            ra_expr.cond,
                            rule_push_down_projections(ra_expr.inputs[1], dd))
    
    elif isinstance(ra_expr, radb.ast.Cross):
        return radb.ast.Cross(rule_push_down_projections(ra_expr.inputs[0], dd),
                             rule_push_down_projections(ra_expr.inputs[1], dd))
    
    return ra_expr


def _maybe_add_projection(ra_expr, needed_attrs, available_attrs, dd):
    """
    Add a projection only if it significantly reduces attributes.
    Special handling: merge projection with existing Select to avoid extra MR job.
    """
    # Don't add projection to base relations (RelRef) - would add an extra MR job
    if isinstance(ra_expr, radb.ast.RelRef):
        return ra_expr
    
    # If the input is a Select, we can combine projection with it (no extra MR job)
    # Otherwise, be conservative about adding projections
    is_select = isinstance(ra_expr, radb.ast.Select)
    
    # Calculate reduction ratio
    if needed_attrs and available_attrs:
        reduction_ratio = len(needed_attrs) / len(available_attrs)
    else:
        reduction_ratio = 1.0
    
    # Add projection if:
    # 1. It's on top of a Select (no extra job because SelectProjectTask combines them), OR
    # 2. It reduces attributes by at least 40% (more aggressive than 50%)
    should_add = is_select or (reduction_ratio <= 0.6 and len(needed_attrs) < len(available_attrs))
    
    if should_add and needed_attrs and len(needed_attrs) < len(available_attrs):
        proj_attrs = []
        for attr_str in sorted(needed_attrs):  # Sort for consistency
            if '.' in attr_str:
                rel, name = attr_str.split('.', 1)
                proj_attrs.append(radb.ast.AttrRef(rel, name))
            else:
                proj_attrs.append(radb.ast.AttrRef(None, attr_str))
        
        optimized_input = rule_push_down_projections(ra_expr, dd)
        return radb.ast.Project(proj_attrs, optimized_input)
    
    return rule_push_down_projections(ra_expr, dd)


def rule_fold_chains(ra_expr):
    """
    Fold chains of operations to reduce the number of MapReduce jobs.
    Specifically, combine Select-Project sequences and consecutive Selects.
    """
    if isinstance(ra_expr, radb.ast.Project):
        input_expr = ra_expr.inputs[0]
        
        # If input is a Select, keep them together (they're already a good chain)
        if isinstance(input_expr, radb.ast.Select):
            # Recursively optimize the input's input
            optimized_select_input = rule_fold_chains(input_expr.inputs[0])
            new_select = radb.ast.Select(input_expr.cond, optimized_select_input)
            return radb.ast.Project(ra_expr.attrs, new_select)
        else:
            return radb.ast.Project(ra_expr.attrs, rule_fold_chains(input_expr))
    
    elif isinstance(ra_expr, radb.ast.Select):
        input_expr = ra_expr.inputs[0]
        
        # If input is also a Select, they're already merged by rule_merge_selections
        # Just recursively optimize the input
        return radb.ast.Select(ra_expr.cond, rule_fold_chains(input_expr))
    
    elif isinstance(ra_expr, radb.ast.Rename):
        return radb.ast.Rename(ra_expr.relname, ra_expr.attrnames, 
                              rule_fold_chains(ra_expr.inputs[0]))
    
    elif isinstance(ra_expr, radb.ast.Join):
        return radb.ast.Join(rule_fold_chains(ra_expr.inputs[0]), 
                            ra_expr.cond,
                            rule_fold_chains(ra_expr.inputs[1]))
    
    elif isinstance(ra_expr, radb.ast.Cross):
        return radb.ast.Cross(rule_fold_chains(ra_expr.inputs[0]),
                             rule_fold_chains(ra_expr.inputs[1]))
    
    return ra_expr
