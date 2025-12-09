#!/usr/bin/env python3
"""
Find which topics are affected by changed files in a PR or commit.

Usage:
    python find_affected_topics.py --base-ref main --head-ref feature-branch
    python find_affected_topics.py --changed-files file1.yaml file2.yaml
"""

import yaml
import os
import sys
import argparse
import subprocess
from pathlib import Path
from typing import Set, List, Dict, Optional


class TopicDependencyAnalyzer:
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.topics_cache = {}
        self.views_to_topics = {}  # Map view name -> set of topics that use it
        self.relationships_file = None
        self._load_all_topics()
        self._build_view_to_topic_map()
        self._find_relationships_file()
    
    def _find_relationships_file(self):
        """Find the relationships.yaml file."""
        relationships_path = self.project_root / "relationships.yaml"
        if relationships_path.exists():
            self.relationships_file = relationships_path
    
    def _load_all_topics(self):
        """Load all topic files and cache them."""
        for topic_path in self.project_root.rglob("*.topic.yaml"):
            try:
                with open(topic_path, 'r') as f:
                    topic = yaml.safe_load(f)
                    if topic:
                        topic_name = topic_path.stem.replace('.topic', '')
                        self.topics_cache[topic_name] = {
                            'path': topic_path,
                            'data': topic,
                            'base_view': topic.get('base_view'),
                            'joins': topic.get('joins', {})
                        }
            except Exception as e:
                print(f"Warning: Could not load topic {topic_path}: {e}", file=sys.stderr)
    
    def _build_view_to_topic_map(self):
        """Build a map of view names to topics that use them."""
        for topic_name, topic_info in self.topics_cache.items():
            base_view = topic_info.get('base_view')
            if base_view:
                if base_view not in self.views_to_topics:
                    self.views_to_topics[base_view] = set()
                self.views_to_topics[base_view].add(topic_name)
            
            # Also check joins
            joins = topic_info.get('joins', {})
            self._extract_views_from_joins(joins, topic_name)
    
    def _extract_views_from_joins(self, joins: Dict, topic_name: str):
        """Recursively extract view names from joins structure."""
        for join_view, nested_joins in joins.items():
            if join_view not in self.views_to_topics:
                self.views_to_topics[join_view] = set()
            self.views_to_topics[join_view].add(topic_name)
            
            if isinstance(nested_joins, dict):
                self._extract_views_from_joins(nested_joins, topic_name)
    
    def _get_view_name_from_file(self, file_path: Path) -> Optional[str]:
        """Get view name from a view file by reading its contents or path."""
        view_name = None
        
        # First, try to read the comment at the top of the file
        try:
            with open(file_path, 'r') as f:
                first_line = f.readline()
                # Look for pattern like: # Reference this view as ecomm__order_items
                if 'Reference this view as' in first_line:
                    # Extract the view name from the comment
                    import re
                    match = re.search(r'as\s+([a-z0-9_]+)', first_line, re.IGNORECASE)
                    if match:
                        view_name = match.group(1)
                        return view_name
        except:
            pass
        
        # Second, try to infer from path structure
        try:
            rel_path = file_path.relative_to(self.project_root)
            stem = rel_path.stem.replace('.view', '').replace('.query', '')
            parts = list(rel_path.parts[:-1])  # All parts except filename
            
            # Common patterns:
            # Ecomm Demo/ECOMM/order_items.view.yaml -> ecomm__order_items
            # Snowflake/DEMO/nba_players.view.yaml -> demo__nba_players
            # Ecomm Demo/DEMO/account.view.yaml -> demo__account
            
            # Check for schema indicators in path
            schema = None
            if 'ECOMM' in parts:
                schema = 'ecomm'
            elif 'DEMO' in parts:
                schema = 'demo'
            elif 'PUBLIC' in parts:
                schema = 'public'
            elif 'Snowflake' in parts:
                # Snowflake/DEMO/... -> demo
                if 'DEMO' in parts:
                    schema = 'demo'
                elif 'PUBLIC' in parts:
                    schema = 'public'
            
            if schema:
                view_name = f"{schema}__{stem}"
            else:
                # Fallback: try to construct from path
                # Convert path parts to lowercase and join with __
                path_parts = [p.lower().replace(' ', '_').replace('-', '_') for p in parts]
                view_name = '__'.join(path_parts + [stem]) if path_parts else stem
        except:
            pass
        
        return view_name
    
    def find_topics_using_view(self, view_name: str) -> Set[str]:
        """Find all topics that use a given view name."""
        affected_topics = set()
        
        # Normalize the view name for matching
        normalized = view_name.lower().strip()
        
        # Try exact match first
        if normalized in self.views_to_topics:
            affected_topics.update(self.views_to_topics[normalized])
        
        # Try variations
        variations = [
            normalized,
            normalized.replace('__', '_'),
            normalized.replace('_', '__'),
            normalized.replace('omni_dbt_', ''),
            f"omni_dbt_{normalized}" if not normalized.startswith('omni_dbt_') else normalized,
        ]
        
        for variation in variations:
            if variation in self.views_to_topics:
                affected_topics.update(self.views_to_topics[variation])
        
        # Also search by partial match (check if view name appears in cached views)
        # This handles cases where the format might differ slightly
        for cached_view, topics in self.views_to_topics.items():
            cached_normalized = cached_view.lower()
            # Check if view_name is contained in cached_view or vice versa
            # But only if they're similar enough (not too generic)
            if (normalized in cached_normalized or cached_normalized in normalized):
                # Extract the base name (last part after __ or _)
                view_base = normalized.split('__')[-1].split('_')[-1]
                cached_base = cached_normalized.split('__')[-1].split('_')[-1]
                if view_base == cached_base:
                    affected_topics.update(topics)
        
        return affected_topics
    
    def find_topics_using_relationships(self) -> Set[str]:
        """Find all topics that might be affected by relationships.yaml changes."""
        # If relationships.yaml changed, all topics could be affected
        # But we can be smarter - check which topics actually use relationships
        # For now, return all topics since relationships affect joins
        return set(self.topics_cache.keys())
    
    def get_changed_files(self, base_ref: str, head_ref: str) -> List[Path]:
        """Get list of changed files between two git refs."""
        try:
            result = subprocess.run(
                ['git', 'diff', '--name-only', '--diff-filter=ACMR', base_ref, head_ref],
                capture_output=True,
                text=True,
                check=True
            )
            changed_files = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    file_path = Path(line)
                    if file_path.exists():
                        changed_files.append(file_path)
            return changed_files
        except subprocess.CalledProcessError as e:
            print(f"Error running git diff: {e}", file=sys.stderr)
            return []
    
    def analyze_changes(self, changed_files: List[str]) -> Set[str]:
        """
        Analyze changed files and return set of affected topic names.
        
        Args:
            changed_files: List of file paths (relative to repo root or absolute)
        
        Returns:
            Set of topic names that need to be regenerated
        """
        affected_topics = set()
        
        for file_str in changed_files:
            file_path = Path(file_str)
            
            # Make path relative to project root if needed
            if not file_path.is_absolute():
                file_path = Path.cwd() / file_path
            elif not file_path.exists() and (self.project_root / file_path).exists():
                file_path = self.project_root / file_path
            
            # Check if it's a topic file
            if file_path.name.endswith('.topic.yaml'):
                topic_name = file_path.stem.replace('.topic', '')
                if topic_name in self.topics_cache:
                    affected_topics.add(topic_name)
                    print(f"Topic file changed: {file_path} -> {topic_name}", file=sys.stderr)
                continue
            
            # Check if it's a view file
            if file_path.name.endswith('.view.yaml') or file_path.name.endswith('.query.view.yaml'):
                view_name = self._get_view_name_from_file(file_path)
                if view_name:
                    topics = self.find_topics_using_view(view_name)
                    affected_topics.update(topics)
                    print(f"View file changed: {file_path} -> {view_name} -> topics: {topics}", file=sys.stderr)
                continue
            
            # Check if it's relationships.yaml
            if file_path.name == 'relationships.yaml':
                topics = self.find_topics_using_relationships()
                affected_topics.update(topics)
                print(f"Relationships file changed -> all topics affected: {len(topics)} topics", file=sys.stderr)
                continue
        
        return affected_topics
    
    def analyze_git_diff(self, base_ref: str, head_ref: str) -> Set[str]:
        """Analyze git diff and return affected topics."""
        changed_files = self.get_changed_files(base_ref, head_ref)
        
        # Filter to only Omni files
        omni_files = []
        project_root_str = str(self.project_root)
        for file_path in changed_files:
            file_str = str(file_path)
            if project_root_str in file_str or 'omni_git/omni' in file_str:
                if (file_path.name.endswith('.topic.yaml') or 
                    file_path.name.endswith('.view.yaml') or 
                    file_path.name.endswith('.query.view.yaml') or
                    file_path.name == 'relationships.yaml'):
                    omni_files.append(file_str)
        
        return self.analyze_changes(omni_files)


def main():
    parser = argparse.ArgumentParser(
        description='Find topics affected by changed files'
    )
    parser.add_argument(
        '--base-ref',
        help='Base git reference (e.g., main, origin/main)'
    )
    parser.add_argument(
        '--head-ref',
        help='Head git reference (e.g., HEAD, feature-branch)'
    )
    parser.add_argument(
        '--changed-files',
        nargs='+',
        help='List of changed file paths'
    )
    parser.add_argument(
        '--project-root',
        default='omni_git/omni',
        help='Root directory of Omni project (default: omni_git/omni)'
    )
    parser.add_argument(
        '--output-format',
        choices=['list', 'json', 'newline'],
        default='newline',
        help='Output format (default: newline - one topic per line)'
    )
    
    args = parser.parse_args()
    
    analyzer = TopicDependencyAnalyzer(args.project_root)
    
    if args.changed_files:
        affected_topics = analyzer.analyze_changes(args.changed_files)
    elif args.base_ref and args.head_ref:
        affected_topics = analyzer.analyze_git_diff(args.base_ref, args.head_ref)
    else:
        print("Error: Must provide either --base-ref and --head-ref, or --changed-files", file=sys.stderr)
        sys.exit(1)
    
    if args.output_format == 'json':
        import json
        print(json.dumps(list(affected_topics)))
    elif args.output_format == 'list':
        print(','.join(sorted(affected_topics)))
    else:  # newline
        for topic in sorted(affected_topics):
            print(topic)
    
    if not affected_topics:
        sys.exit(1)  # Exit with error if no topics found (helps with workflow conditionals)


if __name__ == '__main__':
    main()

