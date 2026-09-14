-- Schema migration script: V1__init_webhook_schema.sql

CREATE TABLE tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    max_rps INT NOT NULL DEFAULT 50,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE webhook_deliveries (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    target_url TEXT NOT NULL,
    payload JSONB NOT NULL,
    secret_key VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    attempts INT DEFAULT 0,
    last_http_status INT,
    last_error_reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_deliveries_tenant_status ON webhook_deliveries(tenant_id, status);
CREATE INDEX idx_deliveries_created_at ON webhook_deliveries(created_at);

-- Pre-seed default tenants
INSERT INTO tenants (id, name, max_rps) VALUES 
('tenant-alpha', 'Alpha Commerce', 50),
('tenant-beta', 'Beta Logistics', 20);
