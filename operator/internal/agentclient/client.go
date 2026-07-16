// Package agentclient dials the pgshard-agent gRPC endpoints that run
// inside every PostgreSQL pod. Controllers poll agents for status (agents
// never write CRD status) and issue lifecycle commands through it.
package agentclient

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

// Pool caches one client connection per agent endpoint. gRPC connections
// self-heal, so entries live until the endpoint is forgotten.
//
// The AgentService contract is mTLS (see proto/pgshard/v1/agent.proto): the
// surface exposes destructive RPCs (ExecSchema runs arbitrary SQL, Promote,
// Fence, RunRestore), so transport auth is the sole authentication. A Pool
// therefore requires credentials; use NewPool with operator-CA mTLS in
// production. NewInsecurePool exists only for in-cluster tests and refuses
// to be the silent default.
type Pool struct {
	creds credentials.TransportCredentials
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPool builds a pool that dials agents with the given transport
// credentials (operator-CA mTLS in production).
func NewPool(creds credentials.TransportCredentials) *Pool {
	return &Pool{creds: creds, conns: map[string]*grpc.ClientConn{}}
}

// NewInsecurePool dials agents in plaintext. Only for tests and local
// development against a fake agent on localhost — never for reaching real
// agents, which are mTLS-only.
func NewInsecurePool() *Pool {
	return NewPool(insecure.NewCredentials())
}

// Get returns an AgentService client for host:port.
func (p *Pool) Get(host string, port int32) (pgshardv1.AgentServiceClient, error) {
	target := fmt.Sprintf("%s:%d", host, port)
	p.mu.Lock()
	defer p.mu.Unlock()
	conn, ok := p.conns[target]
	if !ok {
		var err error
		conn, err = grpc.NewClient(target, grpc.WithTransportCredentials(p.creds))
		if err != nil {
			return nil, fmt.Errorf("dialing agent %s: %w", target, err)
		}
		p.conns[target] = conn
	}
	return pgshardv1.NewAgentServiceClient(conn), nil
}

// Forget drops the cached connection for an endpoint (e.g. pod deleted).
func (p *Pool) Forget(host string, port int32) {
	target := fmt.Sprintf("%s:%d", host, port)
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[target]; ok {
		_ = conn.Close()
		delete(p.conns, target)
	}
}

// Close releases every cached connection.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for target, conn := range p.conns {
		_ = conn.Close()
		delete(p.conns, target)
	}
}
