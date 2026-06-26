package grpc_test

import (
	"context"
	"errors"
	"testing"

	grpcserver "github.com/bete7512/pulse/internal/transport/grpc"
	servicemocks "github.com/bete7512/pulse/internal/service/mocks"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

// errBoom is a generic non-sentinel error used to assert pass-through behaviour.
var errBoom = errors.New("boom")

// ServerSuite is the shared harness for the transport/grpc handler tests. SetupTest gives
// every test a fresh gomock controller, a MockJobService, and a Server wired to it — so the
// thin handlers and the dispatch policy are both exercised against a mocked application layer.
type ServerSuite struct {
	suite.Suite
	ctrl      *gomock.Controller
	svc       *servicemocks.MockJobService
	schedules *servicemocks.MockScheduleService
	srv       *grpcserver.Server
}

func TestServerSuite(t *testing.T) { suite.Run(t, new(ServerSuite)) }

func (s *ServerSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.svc = servicemocks.NewMockJobService(s.ctrl)
	s.schedules = servicemocks.NewMockScheduleService(s.ctrl)
	s.srv = grpcserver.New(s.svc, s.schedules)
}

func ctx() context.Context { return context.Background() }
