from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse


class ExecEnv(Enum):
    LOCAL = 1 
    HDFS = 2 
    MOCK = 3  



class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)




def count_steps(raquery):
    assert (isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery, radb.ast.Project) or
            isinstance(raquery, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join) or isinstance(raquery, radb.ast.Cross):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):

    querystring = luigi.Parameter()

    optimize = luigi.BoolParameter(default=False)

    step = luigi.IntParameter(default=1)


    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=False):
    assert (isinstance(raquery, radb.ast.Node))

    # Optimization: Combine Project(Select(Rename(...))) into a single MR job
    if optimize and isinstance(raquery, radb.ast.Project):
        if isinstance(raquery.inputs[0], radb.ast.Select):
            if isinstance(raquery.inputs[0].inputs[0], radb.ast.Rename):
                # Combined Rename+Select+Project
                return RenameSelectProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)
            else:
                # Combined Select+Project
                return SelectProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.Cross):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    else:
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join) or isinstance(raquery, radb.ast.Cross))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment, optimize=self.optimize)

        return [task1, task2]

    def mapper(self, line):
        relation, tuple_str = line.strip().split('\t')
        json_tuple = json.loads(tuple_str)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        
        # For Cross product (Cartesian product), use a constant key so all tuples join
        if isinstance(raquery, radb.ast.Cross):
            yield (json.dumps("cross_key", sort_keys=True), json.dumps({'relation': relation, 'tuple': json_tuple}))
            return

        condition = raquery.cond

        def get_join_key(json_tuple, condition):
            """Extract the join key value(s) from the tuple based on the condition."""
            if isinstance(condition, radb.ast.ValExprBinaryOp):
                if condition.op == radb.ast.sym.EQ:
                    left_attr = str(condition.inputs[0])
                    right_attr = str(condition.inputs[1])
                    
                    if left_attr in json_tuple:
                        return (left_attr, json_tuple[left_attr])
                    elif right_attr in json_tuple:
                        return (right_attr, json_tuple[right_attr])
                elif condition.op == radb.ast.sym.AND:
                    
                    keys = []
                    for inp in condition.inputs:
                        result = get_join_key(json_tuple, inp)
                        if result:
                            keys.append(result)
                    if keys:
                        key_names = tuple(k[0] for k in keys)
                        key_values = tuple(k[1] for k in keys)
                        return (key_names, key_values)
            return None
        
        key_info = get_join_key(json_tuple, condition)
        if key_info:
            # Just use the second element of key_info as the key value
            key = key_info[1]
            yield (json.dumps(key, sort_keys=True), json.dumps({'relation': relation, 'tuple': json_tuple}))

        

    def reducer(self, key, values):
        # Group tuples by their relation
        tuples_by_relation = {}
        for value in values:
            value_obj = json.loads(value)
            rel = value_obj['relation']
            tup = value_obj['tuple']
            
            if rel not in tuples_by_relation:
                tuples_by_relation[rel] = []
            tuples_by_relation[rel].append(tup)
        
        # Perform the join (cross product of tuples from different relations with the same key)
        if len(tuples_by_relation) >= 2:
            relations = list(tuples_by_relation.keys())
            for tuple1 in tuples_by_relation[relations[0]]:
                for tuple2 in tuples_by_relation[relations[1]]:
                    # Merge the two tuples
                    merged_tuple = {**tuple1, **tuple2}
                    # Use a fixed relation name for join results
                    # This ensures all join results have the same relation name regardless of tuple order
                    relation = "joined"
                    yield (relation, json.dumps(merged_tuple))



class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        condition = radb.parse.one_statement_from_string(self.querystring).cond
        def eval_condition(condition, tuple):
            """Recursively evaluate a condition on a tuple."""
            if isinstance(condition, radb.ast.ValExprBinaryOp):
                if condition.op == radb.ast.sym.EQ:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left == right
                elif condition.op == radb.ast.sym.AND:
                    return all(eval_condition(inp, tuple) for inp in condition.inputs)
                elif condition.op == radb.ast.sym.OR:
                    return any(eval_condition(inp, tuple) for inp in condition.inputs)
                elif condition.op == radb.ast.sym.NE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left != right
                elif condition.op == radb.ast.sym.LT:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left < right
                elif condition.op == radb.ast.sym.LE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left <= right
                elif condition.op == radb.ast.sym.GT:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left > right
                elif condition.op == radb.ast.sym.GE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left >= right
            return False
        
        def eval_expr(expr, tuple):
            if isinstance(expr, radb.ast.AttrRef):
                attr_name = str(expr)
                if attr_name in tuple:
                    return tuple[attr_name]
                for key in tuple.keys():
                    if key.endswith('.' + expr.name):
                        return tuple[key]
                return None
            elif hasattr(expr, 'val'):
                # Convert string constants to appropriate types
                val = expr.val
                # Strip quotes from string constants
                if isinstance(val, str) and len(val) >= 2:
                    if (val[0] == "'" and val[-1] == "'") or (val[0] == '"' and val[-1] == '"'):
                        val = val[1:-1]
                # Try to convert to int, then float, otherwise keep as string
                try:
                    return int(val)
                except ValueError:
                    try:
                        return float(val)
                    except ValueError:
                        return val
            return None
        
        if eval_condition(condition, json_tuple):
            yield (relation, json.dumps(json_tuple))


class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        raquery = radb.parse.one_statement_from_string(self.querystring)

        # Use relname if available (for wildcard *), otherwise use attrnames[0]
        if raquery.relname is not None:
            new_relation = raquery.relname
        else:
            new_relation = raquery.attrnames[0]
        
        new_tuple = {}
        for key, value in json_tuple.items():
            if '.' in key:
                attr_name = key.split('.', 1)[1]
            else:
                attr_name = key
            
            new_key = new_relation + '.' + attr_name
            new_tuple[new_key] = value
        
        yield (new_relation, json.dumps(new_tuple))


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs


        projected_tuple = {}
        
        for attr in attrs:
            attr_str = str(attr)
            if attr_str in json_tuple:
                projected_tuple[attr_str] = json_tuple[attr_str]
            else:
                for key in json_tuple.keys():
                    if key.endswith('.' + attr.name):
                        projected_tuple[key] = json_tuple[key]
                        break
        
        yield (json.dumps(projected_tuple, sort_keys=True), "")


    def reducer(self, key, values):
       

       
        projected_tuple = json.loads(key)
        
        relation = "result"
        if projected_tuple:
            first_key = list(projected_tuple.keys())[0]
            if '.' in first_key:
                relation = first_key.split('.')[0]
        
        yield (relation, key)


class SelectProjectTask(RelAlgQueryTask):
    """Combined Select+Project operation to save one MapReduce job."""

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))
        assert (isinstance(raquery.inputs[0], radb.ast.Select))
        
        # Get the input of the Select operation
        select_input = raquery.inputs[0].inputs[0]
        return [task_factory(select_input, step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        # Parse the query to get both Select and Project operations
        raquery = radb.parse.one_statement_from_string(self.querystring)
        project_attrs = raquery.attrs
        select_cond = raquery.inputs[0].cond
        
        # Evaluate selection condition
        def eval_condition(condition, tuple):
            """Recursively evaluate a condition on a tuple."""
            if isinstance(condition, radb.ast.ValExprBinaryOp):
                if condition.op == radb.ast.sym.EQ:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left == right
                elif condition.op == radb.ast.sym.AND:
                    return all(eval_condition(inp, tuple) for inp in condition.inputs)
                elif condition.op == radb.ast.sym.OR:
                    return any(eval_condition(inp, tuple) for inp in condition.inputs)
                elif condition.op == radb.ast.sym.NE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left != right
                elif condition.op == radb.ast.sym.LT:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left < right
                elif condition.op == radb.ast.sym.LE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left <= right
                elif condition.op == radb.ast.sym.GT:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left > right
                elif condition.op == radb.ast.sym.GE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left >= right
            return False
        
        def eval_expr(expr, tuple):
            if isinstance(expr, radb.ast.AttrRef):
                attr_name = str(expr)
                if attr_name in tuple:
                    return tuple[attr_name]
                for key in tuple.keys():
                    if key.endswith('.' + expr.name):
                        return tuple[key]
                return None
            elif hasattr(expr, 'val'):
                # Convert string constants to appropriate types
                val = expr.val
                # Strip quotes from string constants
                if isinstance(val, str) and len(val) >= 2:
                    if (val[0] == "'" and val[-1] == "'") or (val[0] == '"' and val[-1] == '"'):
                        val = val[1:-1]
                # Try to convert to int, then float, otherwise keep as string
                try:
                    return int(val)
                except ValueError:
                    try:
                        return float(val)
                    except ValueError:
                        return val
            return None
        
        # Apply selection filter
        if not eval_condition(select_cond, json_tuple):
            return
        
        # Apply projection
        projected_tuple = {}
        for attr in project_attrs:
            attr_str = str(attr)
            if attr_str in json_tuple:
                projected_tuple[attr_str] = json_tuple[attr_str]
            else:
                for key in json_tuple.keys():
                    if key.endswith('.' + attr.name):
                        projected_tuple[key] = json_tuple[key]
                        break
        
        yield (json.dumps(projected_tuple, sort_keys=True), "")

    def reducer(self, key, values):
        # Remove duplicates (same as ProjectTask)
        projected_tuple = json.loads(key)
        
        relation = "result"
        if projected_tuple:
            first_key = list(projected_tuple.keys())[0]
            if '.' in first_key:
                relation = first_key.split('.')[0]
        
        yield (relation, key)


class RenameSelectProjectTask(RelAlgQueryTask):
    """Combined Rename+Select+Project operation to save two MapReduce jobs."""

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))
        assert (isinstance(raquery.inputs[0], radb.ast.Select))
        assert (isinstance(raquery.inputs[0].inputs[0], radb.ast.Rename))
        
        # Get the input of the Rename operation
        rename_input = raquery.inputs[0].inputs[0].inputs[0]
        return [task_factory(rename_input, step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        # Parse the query to get Rename, Select, and Project operations
        raquery = radb.parse.one_statement_from_string(self.querystring)
        project_attrs = raquery.attrs
        select_cond = raquery.inputs[0].cond
        rename_op = raquery.inputs[0].inputs[0]
        
        # Apply rename first
        if rename_op.relname is not None:
            new_relation = rename_op.relname
        else:
            new_relation = rename_op.attrnames[0]
        
        renamed_tuple = {}
        for key, value in json_tuple.items():
            if '.' in key:
                attr_name = key.split('.', 1)[1]
            else:
                attr_name = key
            new_key = new_relation + '.' + attr_name
            renamed_tuple[new_key] = value
        
        # Evaluate selection condition on renamed tuple
        def eval_condition(condition, tuple):
            """Recursively evaluate a condition on a tuple."""
            if isinstance(condition, radb.ast.ValExprBinaryOp):
                if condition.op == radb.ast.sym.EQ:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left == right
                elif condition.op == radb.ast.sym.AND:
                    return all(eval_condition(inp, tuple) for inp in condition.inputs)
                elif condition.op == radb.ast.sym.OR:
                    return any(eval_condition(inp, tuple) for inp in condition.inputs)
                elif condition.op == radb.ast.sym.NE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left != right
                elif condition.op == radb.ast.sym.LT:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left < right
                elif condition.op == radb.ast.sym.LE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left <= right
                elif condition.op == radb.ast.sym.GT:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left > right
                elif condition.op == radb.ast.sym.GE:
                    left = eval_expr(condition.inputs[0], tuple)
                    right = eval_expr(condition.inputs[1], tuple)
                    return left >= right
            return False
        
        def eval_expr(expr, tuple):
            if isinstance(expr, radb.ast.AttrRef):
                attr_name = str(expr)
                if attr_name in tuple:
                    return tuple[attr_name]
                for key in tuple.keys():
                    if key.endswith('.' + expr.name):
                        return tuple[key]
                return None
            elif hasattr(expr, 'val'):
                # Convert string constants to appropriate types
                val = expr.val
                # Strip quotes from string constants
                if isinstance(val, str) and len(val) >= 2:
                    if (val[0] == "'" and val[-1] == "'") or (val[0] == '"' and val[-1] == '"'):
                        val = val[1:-1]
                # Try to convert to int, then float, otherwise keep as string
                try:
                    return int(val)
                except ValueError:
                    try:
                        return float(val)
                    except ValueError:
                        return val
            return None
        
        # Apply selection filter on renamed tuple
        if not eval_condition(select_cond, renamed_tuple):
            return
        
        # Apply projection
        projected_tuple = {}
        for attr in project_attrs:
            attr_str = str(attr)
            if attr_str in renamed_tuple:
                projected_tuple[attr_str] = renamed_tuple[attr_str]
            else:
                for key in renamed_tuple.keys():
                    if key.endswith('.' + attr.name):
                        projected_tuple[key] = renamed_tuple[key]
                        break
        
        yield (json.dumps(projected_tuple, sort_keys=True), "")

    def reducer(self, key, values):
        # Remove duplicates (same as ProjectTask)
        projected_tuple = json.loads(key)
        
        relation = "result"
        if projected_tuple:
            first_key = list(projected_tuple.keys())[0]
            if '.' in first_key:
                relation = first_key.split('.')[0]
        
        yield (relation, key)


if __name__ == '__main__':
    luigi.run()
