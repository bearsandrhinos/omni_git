#!/usr/bin/env python3
"""
Generate Terraform resources for Snowflake semantic views from Omni topic YAML files.

Usage:
    python generate_semantic_views.py [topic_file.yaml] [--output-dir terraform/]
    
If no topic file is specified, processes all .topic.yaml files in the project.
"""

import yaml
import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple


class SemanticViewGenerator:
    def __init__(self, project_root: str, output_dir: str = "terraform"):
        self.project_root = Path(project_root)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.views_cache = {}
        self.relationships = []
        self.query_views = set()  # Track which views are query views (derived, not physical tables)
        self.uploaded_tables = set()  # Track which views are uploaded tables (not physical Snowflake tables)
        self._load_relationships()
    
    def tf_string(self, value: str) -> str:
        """Format a string value for Terraform HCL (double-quoted, with proper escaping)."""
        if value is None:
            return '""'
        # Escape backslashes and double quotes
        escaped = value.replace('\\', '\\\\').replace('"', '\\"')
        return f'"{escaped}"'
    
    def tf_sql_string(self, sql: str) -> str:
        """Format a SQL expression for Terraform HCL (double-quoted, with proper escaping).
        SQL string literals use single quotes, which need to be escaped for Terraform.
        Newlines are escaped as \\n for single-line Terraform strings.
        ${...} sequences are escaped as $${...} to prevent Terraform interpolation."""
        if sql is None:
            return '""'
        # Escape ${ as $${ first (before other escapes) to prevent Terraform interpolation
        # Then escape backslashes, double quotes, and newlines
        # SQL single quotes don't need escaping in Terraform strings
        escaped = sql.replace('${', '$${').replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '')
        return f'"{escaped}"'
    
    def _load_relationships(self):
        """Load join relationships from relationships.yaml."""
        relationships_file = self.project_root / "relationships.yaml"
        if relationships_file.exists():
            with open(relationships_file, 'r') as f:
                self.relationships = yaml.safe_load(f) or []
        else:
            print(f"Warning: relationships.yaml not found at {relationships_file}")
        
    def is_query_view(self, view: Dict) -> bool:
        """Check if a view is a query view (derived, not a physical table)."""
        return 'query' in view
    
    def is_uploaded_table(self, view: Dict) -> bool:
        """Check if a view is an uploaded table (not a physical Snowflake table)."""
        return 'uploaded_table_name' in view
    
    def references_query_view(self, sql_expr: str) -> bool:
        """Check if a SQL expression references a query view."""
        if not sql_expr:
            return False
        
        # Check if the SQL expression contains references to any query view
        # Look for ${query_view_name.field} patterns
        import re
        # Pattern to match ${view.field} references
        pattern = r'\$\{([^}]+)\}'
        matches = re.findall(pattern, sql_expr)
        
        for match in matches:
            # Extract the view name (first part before the dot)
            if '.' in match:
                view_ref = match.split('.')[0]
                # Check if this view is a query view
                if view_ref in self.query_views:
                    return True
        
        return False
    
    def load_view(self, view_name: str) -> Optional[Dict]:
        """Load a view YAML file by name (e.g., 'omni_dbt_saas__product').
        Returns the view dict and also tracks if it's a query view."""
        if view_name in self.views_cache:
            return self.views_cache[view_name]
            
        # Search for view files (both .view.yaml and .query.view.yaml)
        view_file = None
        is_query = False
        
        # Convert view_name from omni_dbt_ecomm__order_items to path pattern
        # Try both __ to / conversion and direct matching
        view_name_variants = [
            view_name,  # omni_dbt_ecomm__order_items
            view_name.replace('__', '/'),  # omni_dbt_ecomm/order_items
            view_name.replace('__', '_'),  # omni_dbt_ecomm_order_items
        ]
        
        # First, collect all matching view files and prioritize by comment match
        candidate_files = []
        for view_path in self.project_root.rglob("*.view.yaml"):
            # Check if path matches any variant
            path_str = str(view_path.relative_to(self.project_root))
            view_ref = view_path.stem.replace('.view', '').replace('.query', '')
            
            # Skip semantic view reference files (they're in directories like ECOMM__omni__*)
            # These are generated reference files, not the source view files
            if '__omni__' in path_str:
                continue
            
            # Check the comment in the file for the reference name
            try:
                with open(view_path, 'r') as f:
                    first_line = f.readline()
                    f.seek(0)
                    content = yaml.safe_load(f)
                    
                    # Extract view name from comment line (format: "# Reference this view as {view_name}")
                    comment_view_name = None
                    if first_line and 'Reference this view as' in first_line:
                        # Extract the view name after "as "
                        parts = first_line.split('Reference this view as')
                        if len(parts) > 1:
                            comment_view_name = parts[1].strip()
                    
                    # Check if comment view name exactly matches (highest priority)
                    if comment_view_name == view_name:
                        candidate_files.insert(0, (0, view_path, content))
                    # Check path matches
                    elif (view_ref == view_name or 
                          any(variant in path_str for variant in view_name_variants) or
                          view_name in path_str):
                        if first_line and view_name in first_line:
                            # Contains view name in comment but not exact - medium-high priority
                            candidate_files.append((1, view_path, content))
                        else:
                            # Good path match - medium priority
                            candidate_files.append((2, view_path, content))
            except:
                pass
        
        # Use the first candidate (highest priority, lowest number)
        if candidate_files:
            candidate_files.sort(key=lambda x: x[0])
            _, view_path, view_file = candidate_files[0]
        
        # Also try .query.view.yaml files
        if not view_file:
            for view_path in self.project_root.rglob("*.query.view.yaml"):
                path_str = str(view_path.relative_to(self.project_root))
                view_ref = view_path.stem.replace('.query.view', '')
                
                if (view_ref == view_name or 
                    any(variant in path_str for variant in view_name_variants) or
                    view_name in path_str):
                    with open(view_path, 'r') as f:
                        content = yaml.safe_load(f)
                        view_file = content
                        is_query = True  # Mark as query view
                        break
        
        if view_file:
            # Store both the view and whether it's a query view or uploaded table
            self.views_cache[view_name] = view_file
            # Also store query view and uploaded table flags separately
            if not hasattr(self, 'query_views'):
                self.query_views = set()
            if not hasattr(self, 'uploaded_tables'):
                self.uploaded_tables = set()
            if is_query or self.is_query_view(view_file):
                self.query_views.add(view_name)
            if self.is_uploaded_table(view_file):
                self.uploaded_tables.add(view_name)
        else:
            print(f"Warning: Could not find view file for: {view_name}")
        
        return view_file
    
    def get_snowflake_schema_and_table(self, view: Dict, view_name: str) -> Tuple[str, str]:
        """
        Get the actual Snowflake schema and table name from a view.
        For views with omni_dbt... prefix, schema comes from dbt.config.schema,
        and table_name comes from the table_name field at the top.
        Schema names are returned in uppercase for Snowflake.
        """
        # Check if this is an omni_dbt view
        if view_name.startswith('omni_dbt'):
            # Get schema from dbt.config.schema
            dbt_config = view.get('dbt', {})
            config = dbt_config.get('config', {})
            schema = config.get('schema', 'PUBLIC').upper()  # Uppercase for Snowflake
            
            # Get table name from table_name field at top
            table_name = view.get('table_name', view_name.upper())
            
            return schema, table_name
        else:
            # For non-omni_dbt views, use schema and table_name from top level
            schema = view.get('schema', 'PUBLIC').upper()  # Uppercase for Snowflake
            table_name = view.get('table_name')
            
            # Handle query views and uploaded tables that might not have table_name
            if not table_name:
                # For query views or uploaded tables, use the view name as table name
                # This is a fallback - query views might need special handling
                table_name = view_name.upper().replace('__', '_')
            
            return schema, table_name
    
    def extract_primary_key(self, dimensions: Dict) -> Optional[str]:
        """Extract the primary key dimension."""
        for dim_name, dim_config in dimensions.items():
            if isinstance(dim_config, dict) and dim_config.get('primary_key'):
                return dim_name
        return None
    
    def is_numeric_dimension(self, dim_config: Dict) -> bool:
        """Determine if a dimension should be a fact (numeric/aggregatable) or dimension."""
        # Check format hints
        format_type = dim_config.get('format', '').upper()
        numeric_formats = ['CURRENCY', 'NUMBER', 'PERCENT', 'BIGCURRENCY', 'USDCURRENCY']
        
        # Check if SQL expression suggests numeric
        sql_expr = dim_config.get('sql', '')
        numeric_keywords = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'AMOUNT', 'PRICE', 'REVENUE']
        
        return any(fmt in format_type for fmt in numeric_formats) or \
               any(kw in sql_expr.upper() for kw in numeric_keywords)
    
    def convert_dimension(self, dim_name: str, dim_config: Dict, logical_table_name: str) -> Dict:
        """
        Convert Omni dimension to Snowflake semantic view dimension or fact.
        Dimensions can be either dimensions or facts in Snowflake.
        """
        sql_expr = dim_config.get('sql', f'"{dim_name.upper()}"')
        # Remove quotes and extract column name
        # Handle cases like '"ID"' or '${omni_dbt_saas__opportunities.amount}'
        column = sql_expr.strip('"').strip("'")
        # Remove ${...} wrapper if present
        if column.startswith('${') and column.endswith('}'):
            # Extract just the column part (simplified - may need more parsing)
            parts = column.replace('${', '').replace('}', '').split('.')
            if len(parts) > 1:
                column = parts[-1]
        
        # Determine if this should be a fact (numeric) or dimension
        if self.is_numeric_dimension(dim_config):
            # This is a fact (metric)
            fact_def = {
                'name': dim_name,
                'expression': column,  # For facts, use the column directly
                'table': logical_table_name
            }
            if 'description' in dim_config:
                fact_def['description'] = dim_config['description']
            return {'type': 'fact', 'definition': fact_def}
        else:
            # This is a dimension
            dim_def = {
                'name': dim_name,
                'column': column,
                'table': logical_table_name
            }
            if 'description' in dim_config:
                dim_def['description'] = dim_config['description']
            return {'type': 'dimension', 'definition': dim_def}
    
    def convert_measure(self, measure_name: str, measure_config: Dict, logical_table_name: str) -> Dict:
        """
        Convert Omni measure to Snowflake semantic view metric.
        Measures become metrics in Snowflake semantic views.
        """
        aggregate_type = measure_config.get('aggregate_type', 'sum')
        
        # Map Omni aggregate types to Snowflake expressions
        agg_map = {
            'count': 'COUNT',
            'sum': 'SUM',
            'avg': 'AVG',
            'average': 'AVG',
            'min': 'MIN',
            'max': 'MAX',
            'count_distinct': 'COUNT(DISTINCT)'
        }
        
        sql_func = agg_map.get(aggregate_type.lower(), 'SUM')
        
        # Get the SQL expression for the measure
        sql_expr = measure_config.get('sql', f'"{measure_name.upper()}"')
        
        # Handle complex SQL expressions (may contain ${...} references)
        if sql_expr.startswith('${') and sql_expr.endswith('}'):
            # Extract column reference
            inner = sql_expr.replace('${', '').replace('}', '')
            parts = inner.split('.')
            if len(parts) > 1:
                column = parts[-1]
            else:
                column = inner
        else:
            # Simple column reference
            column = sql_expr.strip('"').strip("'")
        
        # Build metric definition
        metric_def = {
            'name': measure_name,
            'expression': f'{sql_func}({column})',
            'table': logical_table_name
        }
        
        if 'description' in measure_config:
            metric_def['description'] = measure_config['description']
            
        return metric_def
    
    def get_relationships_for_view(self, from_view: str) -> List[Dict]:
        """Get all relationships from relationships.yaml for a given view."""
        matching_rels = []
        for rel in self.relationships:
            if rel.get('join_from_view') == from_view:
                matching_rels.append(rel)
        return matching_rels
    
    def build_join_tree(self, topic: Dict, base_view_name: str) -> List[str]:
        """
        Build a list of all views in the join tree from the topic.
        Returns list of view names including base_view and all joined views.
        """
        views = [base_view_name]
        joins = topic.get('joins', {})
        
        def traverse_joins(join_dict, parent_path=""):
            for join_view, nested_joins in join_dict.items():
                full_path = f"{parent_path}.{join_view}" if parent_path else join_view
                if join_view not in views:
                    views.append(join_view)
                if isinstance(nested_joins, dict):
                    traverse_joins(nested_joins, full_path)
        
        traverse_joins(joins)
        return views
    
    def parse_fields_list(self, topic: Dict, join_tree: List[str]) -> Dict[str, set]:
        """
        Parse the fields list from topic and expand wildcards.
        Returns a dict mapping view_name -> set of field names to include.
        If no fields parameter, returns None (meaning include all fields).
        """
        fields_list = topic.get('fields', [])
        if not fields_list:
            return None  # No fields specified, include all
        
        # Build a set of included fields per view
        included_fields = {}
        all_views_wildcard = False
        
        for field_spec in fields_list:
            if isinstance(field_spec, str):
                # Handle wildcards
                if field_spec == 'ALL_VIEWS.*' or field_spec == 'all_views.*':
                    all_views_wildcard = True
                    continue
                
                # Handle view_name.* wildcard
                if field_spec.endswith('.*'):
                    view_name = field_spec[:-2]  # Remove .*
                    if view_name in join_tree:
                        if view_name not in included_fields:
                            included_fields[view_name] = set()
                        # Mark all fields for this view (we'll check against actual fields later)
                        included_fields[view_name] = None  # None means all fields
                    continue
                
                # Handle specific field: view_name.field_name
                if '.' in field_spec:
                    parts = field_spec.split('.', 1)
                    view_name = parts[0]
                    field_name = parts[1]
                    
                    if view_name in join_tree:
                        if view_name not in included_fields:
                            included_fields[view_name] = set()
                        if included_fields[view_name] is not None:  # Not a wildcard
                            included_fields[view_name].add(field_name)
                else:
                    # Field without view prefix - assume it's from base_view
                    base_view = topic.get('base_view')
                    if base_view:
                        if base_view not in included_fields:
                            included_fields[base_view] = set()
                        if included_fields[base_view] is not None:
                            included_fields[base_view].add(field_spec)
        
        # If ALL_VIEWS.* was specified, include all fields for all views
        if all_views_wildcard:
            for view_name in join_tree:
                included_fields[view_name] = None  # None means all fields
        
        return included_fields
    
    def should_include_field(self, view_name: str, field_name: str, included_fields: Optional[Dict[str, set]]) -> bool:
        """
        Check if a field should be included based on the fields list.
        If included_fields is None, include all fields.
        """
        if included_fields is None:
            return True  # No fields specified, include all
        
        if view_name not in included_fields:
            return False  # View not in fields list
        
        view_fields = included_fields[view_name]
        if view_fields is None:
            return True  # Wildcard for this view, include all fields
        
        return field_name in view_fields
    
    def escape_identifier(self, name: str) -> str:
        """Escape identifier for Snowflake (add quotes)."""
        return f'"{name}"'
    
    def format_qualified_expression(self, table_alias: str, field_name: str) -> str:
        """
        Format qualified expression name for Terraform.
        According to Snowflake SQL docs, identifiers should be UNQUOTED unless they:
        - Start with a number
        - Contain special characters
        - Need to preserve case
        Example: customers.customer_name (not "customers"."customer_name")
        But: customers."25_perc" (must quote if starts with number)
        Snowflake treats unquoted identifiers as uppercase, which matches our uppercase table aliases.
        Format: alias.field or alias."field" if field starts with number
        """
        # Check if field name starts with a number - these must be quoted
        if field_name and field_name[0].isdigit():
            # Field name starts with number, must quote it
            return f'{table_alias}."{field_name}"'
        # Use unquoted identifiers - Snowflake treats them as uppercase
        # This matches the Snowflake SQL syntax shown in their documentation
        return f'{table_alias}.{field_name}'
    
    def convert_omni_granularity(self, sql_expr: str, table_alias: str) -> str:
        """
        Convert Omni granularity syntax to Snowflake SQL.
        [date] -> DATE(...)
        [month] -> DATE_TRUNC('month', ...)
        [quarter] -> DATE_TRUNC('quarter', ...)
        
        Handles patterns like:
        - ${view.field[date]} -> DATE("alias"."field")
        - "field"[date] -> DATE("alias"."field")
        - field[date] -> DATE("alias"."field")
        """
        import re
        
        # Pattern to match ${view.field[granularity]} - granularity is inside the ${}
        # First, find all ${...} blocks and check if they contain [date]
        def replace_date_in_template(match):
            # Match ${view.field[date]}
            full_match = match.group(0)  # e.g., ${omni_dbt_ecomm__order_items.delivered_at[date]}
            if '[date]' in full_match:
                    inner = full_match[2:-1]  # Remove ${ and } -> omni_dbt_ecomm__order_items.delivered_at[date]
                    # Find [date] in the inner string
                    date_pos = inner.find('[date]')
                    if date_pos != -1:
                        field_ref = inner[:date_pos]  # Everything before [date]
                        parts = field_ref.split('.')
                        column = parts[-1] if len(parts) > 1 else field_ref
                        # Use unquoted identifiers per Snowflake SQL syntax
                        return f'DATE({table_alias}.{column.upper()})'
            return full_match  # Return as-is if pattern doesn't match
        
        # Replace ${view.field[date]} patterns
        # First match all ${...} blocks, then replace those containing [date]
        def replace_if_contains_date(match):
            return replace_date_in_template(match)
        
        sql_expr = re.sub(r'\$\{[^}]+\}', replace_date_in_template, sql_expr, flags=re.IGNORECASE | re.DOTALL)
        
        # Pattern to match ${view.field}[date] - granularity is outside the ${}
        def replace_date_after_template(match):
            field_ref = match.group(1)  # The ${view.field} part
            if field_ref.startswith('${') and field_ref.endswith('}'):
                inner = field_ref[2:-1]  # Remove ${ and }
                parts = inner.split('.')
                column = parts[-1] if len(parts) > 1 else inner
                return f'DATE({self.escape_identifier(table_alias)}.{self.escape_identifier(column)})'
            return match.group(0)
        
        sql_expr = re.sub(r'(\$\{[^}]+\})\[date\]', replace_date_after_template, sql_expr, flags=re.IGNORECASE)
        
        # Pattern for quoted or plain column names with [date]
        def replace_date_column(match):
            field_ref = match.group(1)
            if field_ref.startswith('"') and field_ref.endswith('"'):
                col_name = field_ref.strip('"')
                return f'DATE({self.escape_identifier(table_alias)}.{self.escape_identifier(col_name)})'
            else:
                return f'DATE({self.escape_identifier(table_alias)}.{self.escape_identifier(field_ref)})'
        
        sql_expr = re.sub(r'("?[^"\s]+"?)\[date\]', replace_date_column, sql_expr, flags=re.IGNORECASE)
        
        # Similar patterns for [month]
        def replace_month_in_template(match):
            full_match = match.group(0)
            inner = full_match[2:-1]
            if inner.endswith('[month]'):
                field_ref = inner[:-7]
                parts = field_ref.split('.')
                column = parts[-1] if len(parts) > 1 else field_ref
                return f"DATE_TRUNC('month', {self.escape_identifier(table_alias)}.{self.escape_identifier(column)})"
            return full_match
        
        sql_expr = re.sub(r'\$\{[^}]*\[month\][^}]*\}', replace_month_in_template, sql_expr, flags=re.IGNORECASE | re.DOTALL)
        
        def replace_month_after_template(match):
            field_ref = match.group(1)
            if field_ref.startswith('${') and field_ref.endswith('}'):
                inner = field_ref[2:-1]
                parts = inner.split('.')
                column = parts[-1] if len(parts) > 1 else inner
                return f"DATE_TRUNC('month', {self.escape_identifier(table_alias)}.{self.escape_identifier(column)})"
            return match.group(0)
        
        sql_expr = re.sub(r'(\$\{[^}]+\})\[month\]', replace_month_after_template, sql_expr, flags=re.IGNORECASE)
        
        def replace_month_column(match):
            field_ref = match.group(1)
            if field_ref.startswith('"') and field_ref.endswith('"'):
                col_name = field_ref.strip('"')
                return f"DATE_TRUNC('month', {self.escape_identifier(table_alias)}.{self.escape_identifier(col_name)})"
            else:
                return f"DATE_TRUNC('month', {self.escape_identifier(table_alias)}.{self.escape_identifier(field_ref)})"
        
        sql_expr = re.sub(r'("?[^"\s]+"?)\[month\]', replace_month_column, sql_expr, flags=re.IGNORECASE)
        
        # Similar patterns for [quarter]
        def replace_quarter_in_template(match):
            full_match = match.group(0)
            inner = full_match[2:-1]
            quarter_pos = inner.rfind('[quarter]')
            if quarter_pos != -1:
                field_ref = inner[:quarter_pos]
                parts = field_ref.split('.')
                column = parts[-1] if len(parts) > 1 else field_ref
                return f"DATE_TRUNC('quarter', {self.escape_identifier(table_alias)}.{self.escape_identifier(column)})"
            return full_match
        
        sql_expr = re.sub(r'\$\{[^}]*\[quarter\][^}]*\}', replace_quarter_in_template, sql_expr, flags=re.IGNORECASE | re.DOTALL)
        
        def replace_quarter_after_template(match):
            field_ref = match.group(1)
            if field_ref.startswith('${') and field_ref.endswith('}'):
                inner = field_ref[2:-1]
                parts = inner.split('.')
                column = parts[-1] if len(parts) > 1 else inner
                return f"DATE_TRUNC('quarter', {self.escape_identifier(table_alias)}.{self.escape_identifier(column)})"
            return match.group(0)
        
        sql_expr = re.sub(r'(\$\{[^}]+\})\[quarter\]', replace_quarter_after_template, sql_expr, flags=re.IGNORECASE)
        
        def replace_quarter_column(match):
            field_ref = match.group(1)
            if field_ref.startswith('"') and field_ref.endswith('"'):
                col_name = field_ref.strip('"')
                return f"DATE_TRUNC('quarter', {self.escape_identifier(table_alias)}.{self.escape_identifier(col_name)})"
            else:
                return f"DATE_TRUNC('quarter', {self.escape_identifier(table_alias)}.{self.escape_identifier(field_ref)})"
        
        sql_expr = re.sub(r'("?[^"\s]+"?)\[quarter\]', replace_quarter_column, sql_expr, flags=re.IGNORECASE)
        
        return sql_expr
    
    def get_table_alias(self, view_name: str) -> str:
        """Generate a table alias from view name."""
        # Use a clean logical table name (without omni_dbt prefix)
        alias = view_name.replace('omni_dbt_', '').replace('__', '_')
        # Make it shorter and valid identifier
        # Use UPPERCASE to avoid provider adding double quotes (Snowflake treats unquoted as uppercase)
        alias = alias.upper().replace('-', '_')
        return alias
    
    def parse_sql_expression(self, sql_expr: str, table_alias: str, view_to_alias: Optional[Dict[str, str]] = None) -> str:
        """
        Parse SQL expression and convert to Snowflake format.
        Handles ${view.field} format and converts to "alias"."column" format.
        For complex expressions (function calls, etc.), replaces column references within them.
        
        Args:
            sql_expr: SQL expression to parse
            table_alias: Default table alias to use
            view_to_alias: Optional mapping of view names to table aliases for cross-view references
        """
        import re
        
        if view_to_alias is None:
            view_to_alias = {}
        
        # Handle ${view.field} format - simple reference
        if sql_expr.startswith('${') and sql_expr.endswith('}') and '.' not in sql_expr[2:-1]:
            # Simple ${field} reference - use unquoted identifiers (uppercase)
            inner = sql_expr.replace('${', '').replace('}', '')
            return f'{table_alias}.{inner.upper()}'
        
        # Check if this is a simple quoted column reference like "ID" or '"ID"'
        stripped = sql_expr.strip()
        if (stripped.startswith('"') and stripped.endswith('"') and 
            stripped.count('"') == 2 and '(' not in stripped):
            # Simple column reference - strip quotes and use unquoted uppercase
            column = stripped.strip('"').upper()
            return f'{table_alias}.{column}'
        
        # Complex expression - need to replace column references within it
        # Replace ${view.field} patterns - these are Omni template variables
        # We need to map them to actual table aliases and column names
        # Use UNQUOTED identifiers per Snowflake SQL syntax
        def replace_view_field(match):
            field_ref = match.group(1)
            # If it's just a field name (no view prefix), use current table alias
            if '.' not in field_ref:
                # Use unquoted uppercase identifier
                return f'{table_alias}.{field_ref.upper()}'
            # Otherwise, parse the view.field format (e.g., ecomm__order_items.status)
            parts = field_ref.split('.')
            if len(parts) >= 2:
                view_name = parts[0]  # e.g., ecomm__order_items
                column = parts[-1].upper()  # e.g., status -> STATUS
                
                # Check if this is a cross-view reference
                if view_name in view_to_alias:
                    # Use the correct table alias for the referenced view
                    return f'{view_to_alias[view_name]}.{column}'
                else:
                    # View not in mapping, assume current table alias
                    return f'{table_alias}.{column}'
            return match.group(0)  # Return as-is if we can't parse
        
        # Replace ${...} patterns - must do this BEFORE other replacements
        result = re.sub(r'\$\{([^}]+)\}', replace_view_field, sql_expr)
        
        # Fix common SQL syntax issues
        # Replace Python None with SQL NULL (handle both = None and =None)
        result = re.sub(r'\s*=\s*None\b', ' IS NULL', result, flags=re.IGNORECASE)
        # Replace 'not null' string comparison with IS NOT NULL
        result = re.sub(r"\s*=\s*'not null'", ' IS NOT NULL', result, flags=re.IGNORECASE)
        # Also handle cases where None appears in IN clauses or other contexts
        result = re.sub(r'\bNone\b', 'NULL', result, flags=re.IGNORECASE)
        
        # Replace simple quoted column names like "NAME" with qualified names
        # But only if they're not already part of a function call with table alias
        # This is a heuristic - look for quoted identifiers that aren't already qualified
        def replace_quoted_column(match):
            col_name = match.group(1)
            # Don't replace if it's already qualified (has a dot before it)
            before = sql_expr[:match.start()]
            if '.' in before and before.rstrip().endswith('"'):
                return match.group(0)  # Already qualified, leave as-is
            return f'{self.escape_identifier(table_alias)}.{self.escape_identifier(col_name)}'
        
        # For complex expressions with function calls, we need to be more careful
        # In Snowflake semantic views, column references in SQL expressions should be qualified
        # But we need to check if this is a function call - if so, qualify column references inside
        
        # Replace standalone quoted identifiers (not already qualified)
        # Pattern: "COLUMN_NAME" that's not preceded by "alias".
        # But be careful not to replace if it's already part of a qualified reference
        def qualify_column(match):
            col_name = match.group('col')
            # Check if this is already qualified by looking backwards
            before = result[:match.start()]
            # If there's a dot and quote before this, it's already qualified
            if re.search(r'"[^"]*"\s*\.\s*$', before):
                return match.group(0)
            return f'{self.escape_identifier(table_alias)}.{self.escape_identifier(col_name)}'
        
        # Match quoted column names that aren't already qualified
        result = re.sub(r'(?<!\.)"(?P<col>[A-Z_][A-Z0-9_]*)"(?!\.)', qualify_column, result)
        
        return result
    
    def build_filter_condition(self, filter_field: str, filter_op: str, filter_value: Any, view: Dict, table_alias: str) -> str:
        """
        Build a SQL filter condition from Omni filter format.
        
        Args:
            filter_field: Field name to filter on (e.g., 'status', 'age')
            filter_op: Filter operator (e.g., 'is', 'greater_than_or_equal_to')
            filter_value: Filter value (can be single value or list)
            view: The view dictionary to look up field SQL
            table_alias: Table alias to use
        """
        # Find the field in dimensions to get its SQL expression
        dimensions = view.get('dimensions', {})
        field_config = dimensions.get(filter_field, {})
        field_sql = field_config.get('sql', f'"{filter_field.upper()}"')
        
        # Parse the field SQL to get the column reference
        field_expr = self.parse_sql_expression(field_sql, table_alias)
        
        # Map Omni filter operators to SQL operators
        operator_map = {
            'is': '=',
            'is_not': '!=',
            'greater_than': '>',
            'greater_than_or_equal_to': '>=',
            'less_than': '<',
            'less_than_or_equal_to': '<=',
            'contains': 'LIKE',
            'not_contains': 'NOT LIKE'
        }
        
        sql_op = operator_map.get(filter_op, '=')
        
        # Handle special NULL cases
        if filter_value is None or (isinstance(filter_value, str) and filter_value.lower() in ('null', 'none')):
            if filter_op == 'is':
                return f'{field_expr} IS NULL'
            elif filter_op == 'is_not':
                return f'{field_expr} IS NOT NULL'
            else:
                return f'{field_expr} {sql_op} NULL'
        
        # Handle 'not null' string value
        if isinstance(filter_value, str) and filter_value.lower() == 'not null':
            if filter_op == 'is':
                return f'{field_expr} IS NOT NULL'
            elif filter_op == 'is_not':
                return f'{field_expr} IS NULL'
            else:
                return f'{field_expr} {sql_op} \'not null\''
        
        # Handle list values (IN clause or multiple OR conditions)
        if isinstance(filter_value, list):
            if sql_op == '=':
                # Use IN for equality with list
                # SQL string literals use single quotes, escape single quotes as ''
                values_str = ', '.join([f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in filter_value])
                return f'{field_expr} IN ({values_str})'
            elif sql_op == 'LIKE' or sql_op == 'NOT LIKE':
                # For contains with list, use OR conditions
                conditions = []
                for val in filter_value:
                    escaped_val = str(val).replace(chr(39), chr(39)+chr(39))
                    conditions.append(f'{field_expr} {sql_op} \'%{escaped_val}%\'')
                return f'({" OR ".join(conditions)})'
            else:
                # For other operators with list, use OR conditions
                conditions = []
                for val in filter_value:
                    if isinstance(val, str):
                        escaped_val = val.replace(chr(39), chr(39)+chr(39))
                        conditions.append(f'{field_expr} {sql_op} \'{escaped_val}\'')
                    else:
                        conditions.append(f'{field_expr} {sql_op} {val}')
                return f'({" OR ".join(conditions)})'
        else:
            # Single value
            if sql_op in ('LIKE', 'NOT LIKE'):
                escaped_val = str(filter_value).replace(chr(39), chr(39)+chr(39))
                return f'{field_expr} {sql_op} \'%{escaped_val}%\''
            else:
                # Handle boolean values
                if isinstance(filter_value, bool):
                    return f'{field_expr} {sql_op} {str(filter_value).upper()}'
                else:
                    if isinstance(filter_value, str):
                        escaped_val = filter_value.replace(chr(39), chr(39)+chr(39))
                        return f'{field_expr} {sql_op} \'{escaped_val}\''
                    else:
                        return f'{field_expr} {sql_op} {filter_value}'
    
    def build_filter_conditions(self, filters: Dict, view: Dict, table_alias: str) -> str:
        """
        Build SQL WHERE conditions from Omni filters structure.
        
        Args:
            filters: Filters dictionary from measure config
            view: The view dictionary to look up field SQL
            table_alias: Table alias to use
        """
        conditions = []
        
        for filter_field, filter_config in filters.items():
            if not isinstance(filter_config, dict):
                continue
            
            # Handle each filter operator
            for filter_op, filter_value in filter_config.items():
                # Skip time_for_duration filters (Omni-specific, can't translate)
                if filter_op == 'time_for_duration':
                    continue
                condition = self.build_filter_condition(filter_field, filter_op, filter_value, view, table_alias)
                if condition:
                    conditions.append(condition)
        
        # Join all conditions with AND
        return ' AND '.join(conditions) if conditions else ''
    
    def build_filtered_sql(self, base_expr: str, aggregate_type: str, filters: Dict, view: Dict, table_alias: str, primary_key: Optional[str] = None) -> str:
        """
        Build filtered SQL expression using CASE statement.
        
        For COUNT: COUNT(DISTINCT CASE WHEN ... THEN primary_key ELSE NULL END)
        For others: AGG(CASE WHEN ... THEN expression ELSE NULL END)
        """
        filter_conditions = self.build_filter_conditions(filters, view, table_alias)
        
        if not filter_conditions:
            # No filters, return base expression
            return base_expr
        
        # Build CASE statement
        if aggregate_type.lower() in ['count', 'count_distinct']:
            # For COUNT and COUNT_DISTINCT, use the primary key in the CASE
            if primary_key:
                # Get the primary key column reference
                dimensions = view.get('dimensions', {})
                pk_config = dimensions.get(primary_key, {})
                pk_sql = pk_config.get('sql', f'"{primary_key.upper()}"')
                pk_expr = self.parse_sql_expression(pk_sql, table_alias)
                if aggregate_type.lower() == 'count_distinct':
                    case_expr = f'COUNT(DISTINCT CASE WHEN {filter_conditions} THEN {pk_expr} ELSE NULL END)'
                else:
                    case_expr = f'COUNT(DISTINCT CASE WHEN {filter_conditions} THEN {pk_expr} ELSE NULL END)'
            else:
                # Fallback if no primary key
                if aggregate_type.lower() == 'count_distinct':
                    case_expr = f'COUNT(DISTINCT CASE WHEN {filter_conditions} THEN 1 ELSE NULL END)'
                else:
                    case_expr = f'COUNT(CASE WHEN {filter_conditions} THEN 1 ELSE NULL END)'
        else:
            # For other aggregates, wrap the base expression
            case_expr = f'CASE WHEN {filter_conditions} THEN {base_expr} ELSE NULL END'
        
        return case_expr
    
    def generate_terraform_blocks(self, topic: Dict, base_view: Dict, base_view_name: str, database: str, schema: str) -> str:
        """Generate Terraform HCL blocks for Snowflake semantic view."""
        # Build join tree to get all views
        join_tree = self.build_join_tree(topic, base_view_name)
        
        # Parse fields list to determine which fields to include
        included_fields = self.parse_fields_list(topic, join_tree)
        
        blocks = []
        view_to_alias = {}
        skipped_dimensions = set()  # Track dimensions that were skipped (for measures to check against)
        
        # Initialize query_views set if not exists
        if not hasattr(self, 'query_views'):
            self.query_views = set()
        
        # Generate tables blocks and ensure all views have table aliases
        for view_name in join_tree:
            view = self.load_view(view_name)
            if not view:
                # Don't create table alias for views that don't exist - they can't be used in relationships
                # (This handles cases like demo__product_images where the view file doesn't exist)
                continue
            
            # Skip query views and uploaded tables entirely - they don't exist as physical Snowflake tables
            if (self.is_query_view(view) or view_name in self.query_views or
                self.is_uploaded_table(view) or view_name in self.uploaded_tables):
                # Don't create table alias or table block for query views or uploaded tables
                continue
            
            # Get actual Snowflake schema and table
            view_schema, table_name = self.get_snowflake_schema_and_table(view, view_name)
            # Format as fully qualified name for Terraform
            # Terraform expects a string with interpolation for the fully qualified table name
            # Format: "${var.snowflake_database}.\"schema\".\"table\""
            # The database variable is interpolated, schema and table are escaped quotes within the string
            if database.startswith('var.'):
                # For variable references, use Terraform string interpolation
                db_ref = database.replace('var.', 'var.')
                # Format as Terraform string: "${var.database}.\"schema\".\"table\""
                # We need to escape the inner quotes for schema and table names
                full_table_name = f'"${{{db_ref}}}.\\"{view_schema}\\".\\"{table_name}\\""'
            else:
                # Direct database name (all quoted)
                full_table_name = f'"{database}.\\"{view_schema}\\".\\"{table_name}\\""'
            
            # Generate table alias
            table_alias = self.get_table_alias(view_name)
            view_to_alias[view_name] = table_alias
            
            # Build table block
            table_block = f'  tables {{\n'
            # table_alias must be a quoted string in Terraform HCL
            table_block += f'    table_alias = "{table_alias}"\n'
            # table_name is a Terraform string with interpolation, already properly formatted
            table_block += f'    table_name  = {full_table_name}\n'
            
            # Add primary key if available
            dimensions = view.get('dimensions', {})
            primary_key = self.extract_primary_key(dimensions)
            if primary_key:
                # Get the column name from the dimension
                pk_dim = dimensions.get(primary_key, {})
                pk_sql = pk_dim.get('sql', f'"{primary_key.upper()}"')
                pk_column = pk_sql.strip('"').strip("'")
                table_block += f'    primary_key = ["{pk_column}"]\n'
            
            # Add comment if description exists
            description = view.get('description', '')
            if description:
                # Take first line of description
                comment = description.split('\n')[0].strip()
                table_block += f'    comment     = {self.tf_string(comment)}\n'
            
            table_block += f'  }}\n'
            blocks.append(('tables', table_block))
        
        # First pass: identify all dimensions with {{...}} template variables
        # This needs to happen before we process dimensions so we can check references
        for view_name in join_tree:
            if view_name in self.query_views or view_name in self.uploaded_tables:
                continue
            view = self.load_view(view_name)
            if not view:
                continue
            if self.is_query_view(view) or self.is_uploaded_table(view):
                continue
            
            dimensions = view.get('dimensions', {})
            for dim_name, dim_config in dimensions.items():
                if not isinstance(dim_config, dict):
                    continue
                sql_expr = dim_config.get('sql', '')
                if '{{' in str(sql_expr):
                    skipped_dimensions.add(f"{view_name}.{dim_name}")
        
        # Generate dimensions blocks (skip query views and uploaded tables)
        for view_name in join_tree:
            # Skip query views and uploaded tables entirely - they don't exist in Snowflake
            if view_name in self.query_views or view_name in self.uploaded_tables:
                continue
                
            view = self.load_view(view_name)
            if not view:
                continue
            
            # Double-check it's not a query view or uploaded table
            if self.is_query_view(view) or self.is_uploaded_table(view):
                continue
            
            table_alias = view_to_alias[view_name]
            dimensions = view.get('dimensions', {})
            
            for dim_name, dim_config in dimensions.items():
                if not isinstance(dim_config, dict):
                    continue
                
                # Check if this field should be included based on fields list
                if not self.should_include_field(view_name, dim_name, included_fields):
                    continue
                
                # Skip if this is a numeric dimension (will be a fact)
                if self.is_numeric_dimension(dim_config):
                    continue
                
                # Handle dimensions that extend from other dimensions
                extends = dim_config.get('extends', [])
                if extends:
                    # Get the SQL from the parent dimension
                    # extends is a list, typically with one element like [omni_dbt_ecomm__order_items.created_at]
                    parent_ref = extends[0] if extends else None
                    if parent_ref:
                        # Parse the parent reference (e.g., "omni_dbt_ecomm__order_items.created_at")
                        if '.' in parent_ref:
                            parent_view, parent_dim = parent_ref.rsplit('.', 1)
                            # Load the parent view to get the dimension SQL
                            parent_view_obj = self.load_view(parent_view)
                            if parent_view_obj:
                                parent_dims = parent_view_obj.get('dimensions', {})
                                parent_dim_config = parent_dims.get(parent_dim, {})
                                if isinstance(parent_dim_config, dict):
                                    sql_expr = parent_dim_config.get('sql', f'"{parent_dim.upper()}"')
                                else:
                                    sql_expr = f'"{parent_dim.upper()}"'
                            else:
                                # Fallback if parent view not found
                                sql_expr = f'"{parent_dim.upper()}"'
                        else:
                            # Parent dimension is in the same view
                            parent_dims = dimensions
                            parent_dim_config = parent_dims.get(parent_ref, {})
                            if isinstance(parent_dim_config, dict):
                                sql_expr = parent_dim_config.get('sql', f'"{parent_ref.upper()}"')
                            else:
                                sql_expr = f'"{parent_ref.upper()}"'
                    else:
                        sql_expr = dim_config.get('sql', f'"{dim_name.upper()}"')
                else:
                    sql_expr = dim_config.get('sql', f'"{dim_name.upper()}"')
                
                # Skip if this dimension was identified as having {{...}} template variables
                if f"{view_name}.{dim_name}" in skipped_dimensions:
                    continue
                
                # Skip dimensions that reference query views
                if self.references_query_view(sql_expr):
                    continue
                
                # Check if this dimension references another dimension that was skipped (has {{...}} in its SQL)
                # Also check if it references fields from other tables (cross-table references not allowed in dimensions)
                import re
                template_refs = re.findall(r'\$\{([^}]+)\}', str(sql_expr))
                references_skipped = False
                references_other_table = False
                for ref in template_refs:
                    # ref might be like "ecomm__order_items.user_selected_markdate"
                    if '.' in ref:
                        ref_view, ref_field = ref.split('.', 1)
                        # Check if this referenced field is in skipped_dimensions
                        if f"{ref_view}.{ref_field}" in skipped_dimensions:
                            # This dimension references a skipped field, so skip it too
                            references_skipped = True
                            break
                        # Check if this references a different view/table (cross-table reference)
                        # Snowflake semantic views don't allow dimensions to reference other tables
                        if ref_view != view_name and ref_view in view_to_alias:
                            references_other_table = True
                            break
                
                if references_skipped or references_other_table:
                    continue
                
                # Convert Omni granularity syntax [date], [month], [quarter] to Snowflake SQL
                # [date] -> DATE(...), [month] -> DATE_TRUNC('month', ...), [quarter] -> DATE_TRUNC('quarter', ...)
                sql_expr = self.convert_omni_granularity(sql_expr, table_alias)
                
                qualified_expr = self.parse_sql_expression(sql_expr, table_alias, view_to_alias)
                
                # Extract the actual column name from the SQL expression for qualified_expression_name
                # Use the dimension name (lowercase) as the semantic expression name, not the column name
                # The dimension name should be a valid identifier without quotes
                semantic_name = dim_name  # Use the Omni dimension name as the semantic expression name
                
                dim_block = f'  dimensions {{\n'
                dim_block += f'    qualified_expression_name = {self.tf_string(self.format_qualified_expression(table_alias, semantic_name))}\n'
                dim_block += f'    sql_expression            = {self.tf_sql_string(qualified_expr)}\n'
                
                # Add comment if description exists
                description = dim_config.get('description', '')
                if description:
                    comment = description.split('\n')[0].strip()
                    dim_block += f'    comment                   = {self.tf_string(comment)}\n'
                
                # Add synonyms if available
                synonyms = dim_config.get('synonyms', [])
                if synonyms:
                    syn_str = ', '.join([self.tf_string(s) for s in synonyms])
                    dim_block += f'    synonym                   = [{syn_str}]\n'
                
                dim_block += f'  }}\n'
                blocks.append(('dimensions', dim_block))
        
        # Generate facts blocks (from numeric dimensions) - skip query views and uploaded tables
        for view_name in join_tree:
            # Skip query views and uploaded tables entirely - they don't exist in Snowflake
            if view_name in self.query_views or view_name in self.uploaded_tables:
                continue
                
            view = self.load_view(view_name)
            if not view:
                continue
            
            # Double-check it's not a query view or uploaded table
            if self.is_query_view(view) or self.is_uploaded_table(view):
                continue
            
            table_alias = view_to_alias[view_name]
            dimensions = view.get('dimensions', {})
            
            for dim_name, dim_config in dimensions.items():
                if not isinstance(dim_config, dict):
                    continue
                
                # Check if this field should be included based on fields list
                if not self.should_include_field(view_name, dim_name, included_fields):
                    continue
                
                # Only include numeric dimensions as facts
                if not self.is_numeric_dimension(dim_config):
                    continue
                
                sql_expr = dim_config.get('sql', f'"{dim_name.upper()}"')
                
                # Skip facts that reference query views
                if self.references_query_view(sql_expr):
                    continue
                
                # Skip facts with Omni template variables ({{...}}) - but handle [date], [month], [quarter] syntax
                if '{{' in str(sql_expr):
                    continue
                
                # Skip if this fact was identified as having {{...}} template variables
                if f"{view_name}.{dim_name}" in skipped_dimensions:
                    continue
                
                # Check if this fact references another dimension that was skipped (has {{...}} in its SQL)
                # Also check if it references fields from other tables (cross-table references not allowed in facts)
                import re
                template_refs = re.findall(r'\$\{([^}]+)\}', str(sql_expr))
                references_skipped = False
                references_other_table = False
                for ref in template_refs:
                    # ref might be like "ecomm__order_items.user_selected_markdate"
                    if '.' in ref:
                        ref_view, ref_field = ref.split('.', 1)
                        # Check if this referenced field is in skipped_dimensions
                        if f"{ref_view}.{ref_field}" in skipped_dimensions:
                            # This fact references a skipped field, so skip it too
                            references_skipped = True
                            break
                        # Check if this references a different view/table (cross-table reference)
                        # Snowflake semantic views don't allow facts to reference other tables
                        if ref_view != view_name and ref_view in view_to_alias:
                            references_other_table = True
                            break
                
                if references_skipped or references_other_table:
                    continue
                
                # Convert Omni granularity syntax [date], [month], [quarter] to Snowflake SQL
                sql_expr = self.convert_omni_granularity(sql_expr, table_alias)
                
                qualified_expr = self.parse_sql_expression(sql_expr, table_alias, view_to_alias)
                
                fact_block = f'  facts {{\n'
                fact_block += f'    qualified_expression_name = {self.tf_string(self.format_qualified_expression(table_alias, dim_name))}\n'
                fact_block += f'    sql_expression            = {self.tf_sql_string(qualified_expr)}\n'
                
                # Add comment if description exists
                description = dim_config.get('description', '')
                if description:
                    comment = description.split('\n')[0].strip()
                    fact_block += f'    comment                   = {self.tf_string(comment)}\n'
                
                # Add synonyms if available
                synonyms = dim_config.get('synonyms', [])
                if synonyms:
                    syn_str = ', '.join([self.tf_string(s) for s in synonyms])
                    fact_block += f'    synonym                   = [{syn_str}]\n'
                
                fact_block += f'  }}\n'
                blocks.append(('facts', fact_block))
        
        # Generate metrics blocks (from measures) - skip query views and uploaded tables
        for view_name in join_tree:
            # Skip query views and uploaded tables entirely - they don't exist in Snowflake
            if view_name in self.query_views or view_name in self.uploaded_tables:
                continue
                
            view = self.load_view(view_name)
            if not view:
                # Try to get table alias even if view not loaded
                table_alias = view_to_alias.get(view_name)
                if not table_alias:
                    continue
                # Can't process measures without view, skip
                continue
            
            # Double-check it's not a query view or uploaded table
            if self.is_query_view(view) or self.is_uploaded_table(view):
                continue
            
            table_alias = view_to_alias.get(view_name)
            if not table_alias:
                # Should have been set in tables generation, but be safe
                table_alias = self.get_table_alias(view_name)
                view_to_alias[view_name] = table_alias
            
            measures = view.get('measures', {})
            if not measures:
                # No measures in this view, skip
                continue
            
            for measure_name, measure_config in measures.items():
                if not isinstance(measure_config, dict):
                    continue
                
                # Check if this field should be included based on fields list
                if not self.should_include_field(view_name, measure_name, included_fields):
                    continue
                
                sql_expr = measure_config.get('sql', f'"{measure_name.upper()}"')
                
                # Skip measures with Omni template variables ({{...}}) or invalid date filter syntax
                if ('{{' in sql_expr or
                    "'30 days ago'" in sql_expr or "'60 days ago'" in sql_expr or "'30 days'" in sql_expr):
                    continue
                
                # Convert Omni granularity syntax [date], [month], [quarter] to Snowflake SQL
                sql_expr = self.convert_omni_granularity(sql_expr, table_alias)
                
                # Skip measures that reference skipped dimensions
                # Check if the SQL expression references any skipped dimension
                references_skipped_dim = False
                for skipped_dim in skipped_dimensions:
                    dim_view, dim_name = skipped_dim.split('.', 1)
                    # Check if the measure SQL references this dimension
                    if dim_name in sql_expr or f"{dim_view}.{dim_name}" in sql_expr:
                        references_skipped_dim = True
                        break
                
                if references_skipped_dim:
                    continue
                
                # Skip measures that reference other measures/metrics (Snowflake doesn't support metric-to-metric references)
                # Check if SQL expression references other measure names from this view
                other_measure_names = [m for m in measures.keys() if m != measure_name]
                references_other_measure = False
                for other_measure in other_measure_names:
                    # Check if the SQL expression contains a reference to another measure
                    # This is a heuristic - if the measure name appears in the SQL, it's likely a reference
                    if other_measure in sql_expr or other_measure.lower() in sql_expr.lower():
                        references_other_measure = True
                        break
                
                if references_other_measure:
                    continue
                
                aggregate_type = measure_config.get('aggregate_type', 'sum')
                
                # Skip percentile measures - Snowflake semantic views don't support PERCENTILE_CONT/PERCENTILE_DISC
                if aggregate_type.lower() == 'percentile':
                    continue
                
                # Check for filters
                filters = measure_config.get('filters', {})
                
                # Get primary key for COUNT (needed for both filtered and unfiltered COUNT)
                primary_key = None
                if aggregate_type.lower() == 'count':
                    primary_key = self.extract_primary_key(view.get('dimensions', {}))
                
                # For COUNT without SQL, we don't need base_expr
                if aggregate_type.lower() == 'count' and not measure_config.get('sql'):
                    # COUNT without SQL expression
                    if filters:
                        # Build filtered COUNT
                        filtered_expr = self.build_filtered_sql('', aggregate_type, filters, view, table_alias, primary_key)
                        metric_sql = filtered_expr
                    else:
                        # Simple COUNT - use primary key or *
                        if primary_key:
                            dimensions = view.get('dimensions', {})
                            pk_config = dimensions.get(primary_key, {})
                            pk_sql = pk_config.get('sql', f'"{primary_key.upper()}"')
                            pk_expr = self.parse_sql_expression(pk_sql, table_alias)
                            metric_sql = f'COUNT(DISTINCT {pk_expr})'
                        else:
                            metric_sql = 'COUNT(*)'
                else:
                    # Has SQL expression or is not COUNT
                    sql_expr = measure_config.get('sql', f'"{measure_name.upper()}"')
                    
                    # Parse the SQL expression - pass view_to_alias to handle cross-view references
                    base_expr = self.parse_sql_expression(sql_expr, table_alias, view_to_alias)
                    
                    # Build filtered SQL if filters exist
                    if filters:
                        # Check if filters contain time_for_duration (Omni-specific, can't translate)
                        has_time_filter = any(
                            isinstance(filter_config, dict) and 'time_for_duration' in filter_config
                            for filter_config in filters.values()
                        )
                        if has_time_filter:
                            # Skip this measure - it uses Omni-specific date filters
                            continue
                        
                        filtered_expr = self.build_filtered_sql(base_expr, aggregate_type, filters, view, table_alias, primary_key)
                        
                        # For COUNT and COUNT_DISTINCT with filters, the filtered_expr already includes COUNT
                        if aggregate_type.lower() in ['count', 'count_distinct']:
                            metric_sql = filtered_expr
                        else:
                            # Map aggregate types for wrapping
                            agg_map = {
                                'sum': 'SUM',
                                'avg': 'AVG',
                                'average': 'AVG',
                                'min': 'MIN',
                                'max': 'MAX'
                            }
                            sql_func = agg_map.get(aggregate_type.lower(), 'SUM')
                            # For other aggregates, wrap the filtered expression
                            metric_sql = f'{sql_func}({filtered_expr})'
                    else:
                        # No filters - build normal aggregate
                        agg_map = {
                            'count': 'COUNT',
                            'sum': 'SUM',
                            'avg': 'AVG',
                            'average': 'AVG',
                            'min': 'MIN',
                            'max': 'MAX',
                            'count_distinct': 'COUNT(DISTINCT)'
                        }
                        
                        sql_func = agg_map.get(aggregate_type.lower(), 'SUM')
                        
                        # Build the SQL expression
                        if aggregate_type.lower() == 'count_distinct':
                            metric_sql = f'COUNT(DISTINCT {base_expr})'
                        else:
                            metric_sql = f'{sql_func}({base_expr})'
                
                metric_block = f'  metrics {{\n'
                metric_block += f'    semantic_expression {{\n'
                metric_block += f'      qualified_expression_name = {self.tf_string(self.format_qualified_expression(table_alias, measure_name))}\n'
                metric_block += f'      sql_expression            = {self.tf_sql_string(metric_sql)}\n'
                
                # Add comment if description exists
                description = measure_config.get('description', '')
                if description:
                    comment = description.split('\n')[0].strip()
                    metric_block += f'      comment                   = {self.tf_string(comment)}\n'
                
                # Add synonyms if available
                synonyms = measure_config.get('synonyms', [])
                if synonyms:
                    syn_str = ', '.join([self.tf_string(s) for s in synonyms])
                    metric_block += f'      synonym                   = [{syn_str}]\n'
                
                metric_block += f'    }}\n'
                metric_block += f'  }}\n'
                blocks.append(('metrics', metric_block))
        
        # Generate relationships blocks - skip relationships involving query views or uploaded tables
        for view_name in join_tree:
            # Skip query views and uploaded tables entirely - they don't exist in Snowflake
            if view_name in self.query_views or view_name in self.uploaded_tables:
                continue
                
            rels = self.get_relationships_for_view(view_name)
            for rel in rels:
                to_view = rel.get('join_to_view')
                
                # Skip if the target view is also a query view or uploaded table
                if to_view in self.query_views or to_view in self.uploaded_tables:
                    continue
                
                from_alias = view_to_alias.get(view_name)
                to_alias = view_to_alias.get(to_view)
                
                # Skip if either view doesn't have a table alias (view not found, query view, uploaded table, etc.)
                if not from_alias or not to_alias:
                    continue
                
                # Double-check that the target view exists and is valid
                to_view_obj = self.load_view(to_view)
                if to_view_obj and (self.is_query_view(to_view_obj) or self.is_uploaded_table(to_view_obj)):
                    continue
                
                # Parse the on_sql to extract relationship columns
                on_sql = rel.get('on_sql', '')
                # This is a simplified parser - you may need to enhance this
                # Format is typically: ${view.field} = ${view.field}
                # Extract column names from both sides - these should match dimension names
                relationship_columns = []
                referenced_columns = []
                
                # Try to parse ${view.field} = ${view.field}
                if '=' in on_sql:
                    parts = on_sql.split('=')
                    if len(parts) == 2:
                        left = parts[0].strip()
                        right = parts[1].strip()
                        
                        # Extract column from left side (from view)
                        if left.startswith('${') and left.endswith('}'):
                            left_inner = left.replace('${', '').replace('}', '')
                            left_parts = left_inner.split('.')
                            if len(left_parts) > 1:
                                # Get the actual column name from the dimension SQL
                                from_view_obj = self.load_view(view_name)
                                if from_view_obj:
                                    from_dims = from_view_obj.get('dimensions', {})
                                    from_dim_name = left_parts[-1]
                                    from_dim_config = from_dims.get(from_dim_name, {})
                                    if isinstance(from_dim_config, dict):
                                        from_dim_sql = from_dim_config.get('sql', f'"{from_dim_name.upper()}"')
                                        # Extract column name from SQL (remove quotes)
                                        from_col = from_dim_sql.strip('"').strip("'")
                                        relationship_columns.append(from_col)
                                    else:
                                        relationship_columns.append(from_dim_name.upper())
                                else:
                                    relationship_columns.append(left_parts[-1].upper())
                        
                        # Extract column from right side (to view) - this must match the primary key
                        if right.startswith('${') and right.endswith('}'):
                            right_inner = right.replace('${', '').replace('}', '')
                            right_parts = right_inner.split('.')
                            if len(right_parts) > 1:
                                # Get the actual column name from the dimension SQL
                                to_view_obj = self.load_view(to_view)
                                if to_view_obj:
                                    to_dims = to_view_obj.get('dimensions', {})
                                    to_dim_name = right_parts[-1]
                                    to_dim_config = to_dims.get(to_dim_name, {})
                                    if isinstance(to_dim_config, dict):
                                        to_dim_sql = to_dim_config.get('sql', f'"{to_dim_name.upper()}"')
                                        # Extract column name from SQL (remove quotes)
                                        to_col = to_dim_sql.strip('"').strip("'")
                                        referenced_columns.append(to_col)
                                    else:
                                        referenced_columns.append(to_dim_name.upper())
                                else:
                                    referenced_columns.append(right_parts[-1].upper())
                
                # If we couldn't parse, use generic column names
                if not relationship_columns:
                    relationship_columns = ['id']  # Default
                if not referenced_columns:
                    referenced_columns = relationship_columns
                
                rel_block = f'  relationships {{\n'
                rel_block += f'    relationship_identifier = "{from_alias}_to_{to_alias}"\n'
                rel_cols_str = ', '.join([self.tf_string(col) for col in relationship_columns])
                rel_block += f'    relationship_columns    = [{rel_cols_str}]\n'
                rel_block += f'    table_name_or_alias {{\n'
                # table_alias must be a quoted string in Terraform HCL
                rel_block += f'      table_alias = "{from_alias}"\n'
                rel_block += f'    }}\n'
                ref_cols_str = ', '.join([self.tf_string(col) for col in referenced_columns])
                rel_block += f'    referenced_relationship_columns = [{ref_cols_str}]\n'
                rel_block += f'    referenced_table_name_or_alias {{\n'
                # table_alias must be a quoted string in Terraform HCL
                rel_block += f'      table_alias = "{to_alias}"\n'
                rel_block += f'    }}\n'
                rel_block += f'  }}\n'
                blocks.append(('relationships', rel_block))
        
        # Combine all blocks
        return '\n'.join([block for _, block in blocks])
    
    def find_topic_by_name(self, topic_name: str) -> Optional[Path]:
        """Find a topic file by name (without .topic.yaml extension)."""
        # Search for topic files matching the name
        # Skip topics in Snowflake folder (they use PUBLIC schema)
        candidates = []
        for topic_path in self.project_root.rglob("*.topic.yaml"):
            # Skip topics in Snowflake folder
            if 'Snowflake' in topic_path.parts:
                continue
            # Check if the topic name matches (with or without directory path)
            if topic_path.stem.replace('.topic', '') == topic_name:
                candidates.append(topic_path)
            # Also check if the filename contains the topic name
            elif topic_name in topic_path.stem.replace('.topic', ''):
                candidates.append(topic_path)
        
        # Return first candidate (prefer exact matches)
        if candidates:
            return candidates[0]
        
        return None
    
    def generate_terraform_resource(self, topic_path: Path, topic: Dict, topic_name: Optional[str] = None) -> str:
        """Generate Terraform resource for a semantic view."""
        base_view_name = topic.get('base_view')
        if not base_view_name:
            raise ValueError(f"Topic {topic_path} missing 'base_view' field")
        
        # Load the base view
        base_view = self.load_view(base_view_name)
        if not base_view:
            raise ValueError(f"Could not find view file for: {base_view_name}")
        
        # Get actual Snowflake schema and table (for the schema where semantic view will be created)
        schema, _ = self.get_snowflake_schema_and_table(base_view, base_view_name)
        
        # Skip topics that use PUBLIC schema (tables don't exist in PUBLIC schema)
        if schema == 'PUBLIC':
            raise ValueError(f"Topic {topic_path} uses PUBLIC schema which is not supported. Skipping.")
        
        # Generate semantic view name using naming convention: omni_[topic_filename]_sv
        # Always use just the filename (without directory path) from the topic file
        clean_topic_name = topic_path.stem.replace('.topic', '').replace('__', '_').replace('-', '_')
        semantic_view_name = f"omni_{clean_topic_name}_sv"
        
        # Use Terraform variable so it reads from terraform.tfvars
        database = "var.snowflake_database"
        
        # Generate the Terraform blocks
        blocks = self.generate_terraform_blocks(topic, base_view, base_view_name, database, schema)
        
        # Add comment if topic has description
        comment_block = ""
        description = topic.get('description', '')
        if description:
            comment = description.split('\n')[0].strip()
            comment_block = f'  comment  = {self.tf_string(comment)}\n\n'
        
        # Generate Terraform resource
        # Note: Terraform will replace existing views automatically when the resource definition changes
        # If a view already exists, use: terraform import or terraform destroy -target first
        tf_resource = f'''resource "snowflake_semantic_view" "{semantic_view_name}" {{
  database = {database}
  schema   = "{schema}"
  name     = "{semantic_view_name}"
{comment_block}{blocks}
}}
'''
        return tf_resource
    
    def process_topic(self, topic_path: Path, topic_name: Optional[str] = None) -> str:
        """Process a single topic file and generate Terraform resource."""
        with open(topic_path, 'r') as f:
            topic = yaml.safe_load(f)
        
        try:
            tf_resource = self.generate_terraform_resource(topic_path, topic, topic_name)
            return tf_resource
        except Exception as e:
            return f"# Error processing {topic_path}: {str(e)}\n"
    
    def process_all_topics(self) -> None:
        """Process all topic files in the project."""
        topic_files = list(self.project_root.rglob("*.topic.yaml"))
        
        if not topic_files:
            print("No topic files found.")
            return
        
        # Generate main.tf with all resources
        main_tf_content = "# Generated Terraform resources for Snowflake semantic views\n"
        main_tf_content += "# Generated from Omni topic YAML files\n\n"
        
        for topic_path in sorted(topic_files):
            # Skip topics in Snowflake folder (they use PUBLIC schema which doesn't exist)
            if 'Snowflake' in topic_path.parts:
                print(f"Skipping {topic_path} (Snowflake folder - uses PUBLIC schema)")
                continue
            
            print(f"Processing {topic_path}...")
            topic_name = topic_path.stem.replace('.topic', '')
            try:
                tf_resource = self.process_topic(topic_path, topic_name)
                main_tf_content += tf_resource + "\n"
            except ValueError as e:
                if 'PUBLIC schema' in str(e):
                    print(f"Skipping {topic_path}: {e}")
                    continue
                raise
        
        # Write to terraform/main.tf
        output_file = self.output_dir / "main.tf"
        with open(output_file, 'w') as f:
            f.write(main_tf_content)
        
        print(f"\nGenerated Terraform resources in {output_file}")
        print(f"Total topics processed: {len(topic_files)}")
    
    def process_single_topic(self, topic_path: Path, topic_name: Optional[str] = None) -> None:
        """Process a single topic file."""
        if topic_name is None:
            topic_name = topic_path.stem.replace('.topic', '')
        
        # Skip topics in Snowflake folder (they use PUBLIC schema which doesn't exist)
        if 'Snowflake' in topic_path.parts:
            print(f"Skipping {topic_path} (Snowflake folder - uses PUBLIC schema)")
            return
        
        try:
            tf_resource = self.process_topic(topic_path, topic_name)
        except ValueError as e:
            if 'PUBLIC schema' in str(e):
                print(f"Skipping {topic_path}: {e}")
                return
            raise
        
        # Use semantic view name for output file - always use just the filename
        clean_topic_name = topic_path.stem.replace('.topic', '').replace('__', '_').replace('-', '_')
        semantic_view_name = f"omni_{clean_topic_name}_sv"
        output_file = self.output_dir / f"{semantic_view_name}.tf"
        
        with open(output_file, 'w') as f:
            f.write(tf_resource)
        
        print(f"Generated Terraform resource for semantic view '{semantic_view_name}' in {output_file}")
    
    def process_topic_by_name(self, topic_name: str) -> None:
        """Process a topic by name (finds the topic file automatically)."""
        topic_path = self.find_topic_by_name(topic_name)
        if not topic_path:
            print(f"Error: Topic '{topic_name}' not found.")
            print(f"Searched in: {self.project_root}")
            sys.exit(1)
        
        print(f"Found topic file: {topic_path}")
        self.process_single_topic(topic_path, topic_name)


def main():
    parser = argparse.ArgumentParser(
        description='Generate Terraform resources for Snowflake semantic views from Omni topics'
    )
    parser.add_argument(
        'topic',
        nargs='?',
        help='Topic name (e.g., "opportunity") or path to topic YAML file. If not provided, processes all topics.'
    )
    parser.add_argument(
        '--output-dir',
        default='terraform',
        help='Output directory for Terraform files (default: terraform)'
    )
    parser.add_argument(
        '--project-root',
        default='my-omni-project',
        help='Root directory of the Omni project (default: my-omni-project)'
    )
    
    args = parser.parse_args()
    
    generator = SemanticViewGenerator(
        project_root=args.project_root,
        output_dir=args.output_dir
    )
    
    if args.topic:
        # Check if it's a file path or topic name
        topic_path = Path(args.topic)
        if topic_path.exists() and topic_path.suffix in ['.yaml', '.yml']:
            # It's a file path
            topic_name = topic_path.stem.replace('.topic', '')
            generator.process_single_topic(topic_path, topic_name)
        else:
            # It's a topic name - find the file
            generator.process_topic_by_name(args.topic)
    else:
        generator.process_all_topics()


if __name__ == '__main__':
    main()

