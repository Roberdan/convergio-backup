//! MCP tool definitions for the backup extension.

use convergio_types::extension::McpToolDef;
use serde_json::json;

pub fn backup_tools() -> Vec<McpToolDef> {
    vec![
        McpToolDef {
            name: "cvg_list_snapshots".into(),
            description: "List backup snapshots.".into(),
            method: "GET".into(),
            path: "/api/backup/snapshots".into(),
            input_schema: json!({"type": "object", "properties": {}}),
            min_ring: "community".into(),
            path_params: vec![],
        },
        McpToolDef {
            name: "cvg_create_snapshot".into(),
            description: "Create a new backup snapshot.".into(),
            method: "POST".into(),
            path: "/api/backup/snapshots/create".into(),
            input_schema: json!({"type": "object", "properties": {"label": {"type": "string", "description": "Snapshot label"}}}),
            min_ring: "trusted".into(),
            path_params: vec![],
        },
        McpToolDef {
            name: "cvg_verify_snapshot".into(),
            description: "Verify integrity of a backup snapshot.".into(),
            method: "POST".into(),
            path: "/api/backup/snapshots/verify".into(),
            input_schema: json!({"type": "object", "properties": {"id": {"type": "string", "description": "Snapshot ID to verify"}}, "required": ["id"]}),
            min_ring: "trusted".into(),
            path_params: vec![],
        },
        McpToolDef {
            name: "cvg_list_retention_rules".into(),
            description: "List backup retention rules.".into(),
            method: "GET".into(),
            path: "/api/backup/retention/rules".into(),
            input_schema: json!({"type": "object", "properties": {}}),
            min_ring: "community".into(),
            path_params: vec![],
        },
        McpToolDef {
            name: "cvg_set_retention_rules".into(),
            description: "Set backup retention rules.".into(),
            method: "POST".into(),
            path: "/api/backup/retention/rules".into(),
            input_schema: json!({"type": "object", "properties": {"max_snapshots": {"type": "integer"}, "max_age_days": {"type": "integer"}}}),
            min_ring: "trusted".into(),
            path_params: vec![],
        },
        McpToolDef {
            name: "cvg_purge_backups".into(),
            description: "Purge old backup snapshots per retention rules.".into(),
            method: "POST".into(),
            path: "/api/backup/retention/purge".into(),
            input_schema: json!({"type": "object", "properties": {}}),
            min_ring: "trusted".into(),
            path_params: vec![],
        },
        McpToolDef {
            name: "cvg_purge_log".into(),
            description: "Get backup purge log.".into(),
            method: "GET".into(),
            path: "/api/backup/purge-log".into(),
            input_schema: json!({"type": "object", "properties": {}}),
            min_ring: "community".into(),
            path_params: vec![],
        },
        McpToolDef {
            name: "cvg_export_backup".into(),
            description: "Export data to a backup archive.".into(),
            method: "POST".into(),
            path: "/api/backup/export".into(),
            input_schema: json!({"type": "object", "properties": {}}),
            min_ring: "trusted".into(),
            path_params: vec![],
        },
    ]
}
