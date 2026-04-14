import sqlparse
import radb.ast
import radb.parse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, Whitespace, Wildcard


def translate(stmt):
    
    select_list = _extract_select_list(stmt)
    
    from_list = _extract_from_list(stmt)
    
    where_atoms = _extract_where_atoms(stmt)
    
    from_expr = _build_from_expression(from_list)

    
    if where_atoms:
        where_str = " and ".join(f"({a})" if " and " in a else a for a in where_atoms)
       
        needs_paren = len(from_list) > 1
        if needs_paren:
            sel_expr = f"\\select_{{{where_str}}} ({from_expr})"
        else:
            sel_expr = f"\\select_{{{where_str}}} {from_expr}"
    else:
        sel_expr = from_expr

    
    if select_list == ['*']:
        
        ra_str = sel_expr + ";"
    else:
        proj_attrs = ", ".join(select_list)
        
        if where_atoms or len(from_list) > 1:
            ra_str = f"\\project_{{{proj_attrs}}} ({sel_expr});"
        else:
            ra_str = f"\\project_{{{proj_attrs}}} {sel_expr};"

    
    ra = radb.parse.one_statement_from_string(ra_str)
    return ra


def _extract_select_list(stmt):
    """Extract the list of attributes from SELECT clause."""
    select_seen = False
    result = []
    
    for token in stmt.tokens:
        
        if token.ttype is Keyword and token.value.upper() in ('SELECT', 'DISTINCT'):
            select_seen = True
            continue
            
        if select_seen:
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                break
            if token.ttype is Whitespace:
                continue
            if token.ttype is Wildcard:
                return ['*']
            if isinstance(token, IdentifierList):
                return [str(ident).strip() for ident in token.get_identifiers()]
            if isinstance(token, Identifier):
                result.append(str(token).strip())
                
            elif token.ttype is None:
                
                text = str(token).strip()
                if text and text != ',':
                    result.append(text)
    
   
    return result if result else ['*']


def _extract_from_list(stmt):
    """Extract list of (table_name, alias_or_None) from FROM clause."""
    from_seen = False
    from_list = []
    
    for token in stmt.tokens:
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
            continue
        if from_seen:
            if isinstance(token, Where) or (token.ttype is Keyword and token.value.upper() == 'WHERE'):
                break
            if token.ttype is Whitespace:
                continue
            if isinstance(token, IdentifierList):
                for ident in token.get_identifiers():
                    from_list.append(_parse_table_alias(ident))
            elif isinstance(token, Identifier):
                from_list.append(_parse_table_alias(token))
    
    return from_list


def _parse_table_alias(token):
    """Parse a table reference, possibly with an alias."""
    text = str(token).strip()
    parts = text.split()
    
    
    parts = [p for p in parts if p.upper() != 'AS']
    
    if len(parts) == 2:
        return (parts[0], parts[1])
    elif len(parts) == 1: 
        return (parts[0], None)
    else:
        return (text, None)


def _extract_where_atoms(stmt):
    """Extract WHERE conditions as list of equality strings."""
    for token in stmt.tokens:
        if isinstance(token, Where):
            return _parse_where_clause(token)
    return []


def _parse_where_clause(where_token):
    """Parse WHERE clause and extract individual conditions."""
    where_text = str(where_token)[5:].strip() 
    conditions = []
    import re
    parts = re.split(r'\s+and\s+', where_text, flags=re.IGNORECASE)
    
    for part in parts:
        part = part.strip()
        if part:
            part = ' '.join(part.split())
            conditions.append(part)
    
    return conditions


def _build_from_expression(from_list):
    """Build FROM expression with cross products and renames."""
    if not from_list:
        raise ValueError("No tables in FROM clause")

    table_name, alias = from_list[0]
    if alias:
        expr = f"\\rename_{{{alias}}}({table_name})"
    else:
        expr = table_name
    
    # Add cross products for remaining tables
    for i in range(1, len(from_list)):
        table_name, alias = from_list[i]
        if alias:
            right_expr = f"\\rename_{{{alias}}}({table_name})"
        else:
            right_expr = table_name
        
        expr = f"{expr} \\cross {right_expr}"
    
    return expr