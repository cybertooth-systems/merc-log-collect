CREATE TABLE IF NOT EXISTS logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts CHAR(100) NOT NULL,
    node_id CHAR(100),
    rev_id CHAR(100),
    parent_ids CHAR(100),
    author CHAR(255),
    tags CHAR(255),
    branch CHAR(100),
    diffstat CHAR(255),
    files TEXT,
    graph_node CHAR(10),
    repo_path CHAR(255)
);

CREATE TABLE IF NOT EXISTS errs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts CHAR(100) NOT NULL,
    err TEXT,
    repo_path CHAR(255)
);
