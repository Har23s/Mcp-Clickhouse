#!/usr/bin/env python3
"""
ClickHouse MCP Tools
Provides tools for interacting with ClickHouse databases
"""

import json
import logging
from typing import Dict, Any, Optional
import clickhouse_connect

logger = logging.getLogger(__name__)


class ClickHouseConnection:
    """Manages ClickHouse database connection"""
    
    def __init__(self, host: str, username: str, password: str, port: int = 8123, secure: bool = False):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.secure = secure
        self._client = None
    
    def get_client(self):
        """Get or create ClickHouse client"""
        if self._client is None:
            try:
                self._client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    secure=self.secure,
                    verify=False  # Set to True in production
                )
                logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                raise
        return self._client
    
    def test_connection(self) -> bool:
        """Test if connection is working"""
        try:
            client = self.get_client()
            result = client.query("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


class RunSelectQuery:
    """Tool for executing SELECT queries on ClickHouse"""
    
    def __init__(self, connection: ClickHouseConnection):
        self.name = "run_select_query"
        self.description = "Execute SELECT SQL queries on ClickHouse database. Queries are executed in readonly mode for safety."
        self.connection = connection
    
    def get_input_schema(self) -> Dict[str, Any]:
        """Define custom input schema for this tool"""
        return {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL SELECT query to execute"
                }
            },
            "required": ["sql"]
        }
    
    def execute(self, sql: str) -> str:
        """Execute a SELECT query"""
        try:
            # Ensure query is readonly
            sql_lower = sql.strip().lower()
            if not sql_lower.startswith('select') and not sql_lower.startswith('show') and not sql_lower.startswith('describe'):
                return json.dumps({
                    "error": "Only SELECT, SHOW, and DESCRIBE queries are allowed"
                })
            
            client = self.connection.get_client()
            result = client.query(sql)
            
            # Convert result to list of dictionaries
            columns = result.column_names
            rows = []
            for row in result.result_rows:
                rows.append(dict(zip(columns, row)))
            
            return json.dumps({
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "query": sql
            }, indent=2, default=str)
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return json.dumps({
                "error": f"Query execution failed: {str(e)}",
                "query": sql
            })


class ListDatabases:
    """Tool for listing all databases"""
    
    def __init__(self, connection: ClickHouseConnection):
        self.name = "list_databases"
        self.description = "List all databases available in the ClickHouse cluster"
        self.connection = connection
    
    def execute(self) -> str:
        """List all databases"""
        try:
            client = self.connection.get_client()
            result = client.query("SHOW DATABASES")
            
            databases = [row[0] for row in result.result_rows]
            
            return json.dumps({
                "success": True,
                "databases": databases,
                "count": len(databases)
            }, indent=2)
            
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            return json.dumps({
                "error": f"Failed to list databases: {str(e)}"
            })


class ListTables:
    """Tool for listing tables in a database"""
    
    def __init__(self, connection: ClickHouseConnection):
        self.name = "list_tables"
        self.description = "List all tables in a specific database"
        self.connection = connection
    
    def get_input_schema(self) -> Dict[str, Any]:
        """Define custom input schema for this tool"""
        return {
            "type": "object",
            "properties": {
                "database": {
                    "type": "string",
                    "description": "The name of the database to list tables from"
                }
            },
            "required": ["database"]
        }
    
    def execute(self, database: str) -> str:
        """List tables in a database"""
        try:
            client = self.connection.get_client()
            result = client.query(f"SHOW TABLES FROM {database}")
            
            tables = [row[0] for row in result.result_rows]
            
            return json.dumps({
                "success": True,
                "database": database,
                "tables": tables,
                "count": len(tables)
            }, indent=2)
            
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return json.dumps({
                "error": f"Failed to list tables: {str(e)}",
                "database": database
            })


class DescribeTable:
    """Tool for describing table structure"""
    
    def __init__(self, connection: ClickHouseConnection):
        self.name = "describe_table"
        self.description = "Get the structure/schema of a specific table"
        self.connection = connection
    
    def get_input_schema(self) -> Dict[str, Any]:
        """Define custom input schema for this tool"""
        return {
            "type": "object",
            "properties": {
                "database": {
                    "type": "string",
                    "description": "The name of the database"
                },
                "table": {
                    "type": "string",
                    "description": "The name of the table"
                }
            },
            "required": ["database", "table"]
        }
    
    def execute(self, database: str, table: str) -> str:
        """Describe table structure"""
        try:
            client = self.connection.get_client()
            result = client.query(f"DESCRIBE TABLE {database}.{table}")
            
            columns = result.column_names
            schema = []
            for row in result.result_rows:
                schema.append(dict(zip(columns, row)))
            
            return json.dumps({
                "success": True,
                "database": database,
                "table": table,
                "schema": schema
            }, indent=2, default=str)
            
        except Exception as e:
            logger.error(f"Failed to describe table: {e}")
            return json.dumps({
                "error": f"Failed to describe table: {str(e)}",
                "database": database,
                "table": table
            })


class ClickHouseToolHandler:
    """Handler for ClickHouse MCP tools"""
    
    def __init__(self, host: str, username: str, password: str, port: int = 8123, secure: bool = False):
        self.connection = ClickHouseConnection(host, username, password, port, secure)
        self.run_query = RunSelectQuery(self.connection)
        self.list_databases = ListDatabases(self.connection)
        self.list_tables = ListTables(self.connection)
        self.describe_table = DescribeTable(self.connection)
    
    def test_connection(self) -> bool:
        """Test database connection"""
        return self.connection.test_connection()
    
    def get_available_tools(self) -> Dict[str, Any]:
        """Get available tools information"""
        return {
            self.run_query.name: {
                "name": self.run_query.name,
                "description": self.run_query.description
            },
            self.list_databases.name: {
                "name": self.list_databases.name,
                "description": self.list_databases.description
            },
            self.list_tables.name: {
                "name": self.list_tables.name,
                "description": self.list_tables.description
            },
            self.describe_table.name: {
                "name": self.describe_table.name,
                "description": self.describe_table.description
            }
        }
    
    def call_tool(self, name: str, **kwargs) -> str:
        """Call a tool by name with parameters"""
        if name == self.run_query.name:
            sql = kwargs.get('sql')
            return self.run_query.execute(sql)
        elif name == self.list_databases.name:
            return self.list_databases.execute()
        elif name == self.list_tables.name:
            database = kwargs.get('database')
            return self.list_tables.execute(database)
        elif name == self.describe_table.name:
            database = kwargs.get('database')
            table = kwargs.get('table')
            return self.describe_table.execute(database, table)
        else:
            return json.dumps({"error": f"Unknown tool: {name}"})